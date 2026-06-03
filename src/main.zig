const std = @import("std");
const assert = std.debug.assert;
const Io = std.Io;
const Duration = Io.Duration;
const net = Io.net;
const log = std.log;
const fatal = std.process.fatal;
const Allocator = std.mem.Allocator;
const NetworkConfig = @import("network_config");
const port_list = NetworkConfig.port_list;
const MAX_NODE = port_list.len;
var memberships: MembershipList = undefined;
var self_id: Id = undefined;
var self_gen: u32 = 1;

const Id = u32;

const Membership = struct {
    ping_addr: net.IpAddress,
    multicast_addr: net.IpAddress,
    status: std.atomic.Value(NodeStatus),
    pending_ack: std.atomic.Value(i8),
};
const MembershipList = [MAX_NODE]Membership;

fn print_memberships(term: Io.Terminal, ships: MembershipList) !void {
    const stdout = term.writer;
    try stdout.print("{s: <10}{s: <10}\n", .{ "id", "status" });
    for (ships, 0..) |member, id| {
        const id_color: Io.Terminal.Color = if (self_id == id) .blue else .reset;
        print_color(term, id_color, "{: <10}", .{ id });
        const status = member.status.load(.unordered);
        const status_color: Io.Terminal.Color = switch (status) {
            .alive => .green,
            .suspected => .yellow,
            .dead => .red,
        };
        print_color(term, status_color, "{s: <10}\n", .{ @tagName(status) });
        // try stdout.print("{: <10}{: <10}\n", .{ member.last_heard.toSeconds(), member.gen });
    }
}

const NodeStatus = enum(u8) {
    dead,
    suspected,
    alive,

    pub const NETWORK_ROUNDTRIP_TIME = Duration.fromMilliseconds(1000);
    pub const SUSPECTED_TIMEOUT = Duration { .nanoseconds = NETWORK_ROUNDTRIP_TIME.nanoseconds * 1 };
    pub const DEAD_TIMEOUT = Duration { .nanoseconds = NETWORK_ROUNDTRIP_TIME.nanoseconds * 2 };
    comptime { assert(DEAD_TIMEOUT.nanoseconds > SUSPECTED_TIMEOUT.nanoseconds); }
    pub const PING_INTERVAL = Duration.fromMilliseconds(500);
};

const Terminal = struct {
    const clear = "\x1b[1J";
    const move_up = "\x1b[1A";
};

const Message = struct {
    from: Id,
    type: Type,
    pub const Type = enum(u8) {
        ping,
        ack,
    };

    pub const MESSAGE_SIZE = @sizeOf(Message);

    pub fn serialize(msg: Message) [MESSAGE_SIZE]u8 {
        return std.mem.asBytes(&msg).*;
    }

    pub fn deserialize(bytes: []const u8) Message {
        assert(bytes.len == MESSAGE_SIZE);
        return std.mem.bytesToValue(Message, bytes);
    }

    pub fn recv(io: Io, sock: *const net.Socket) !Message {
        var buf: [Message.MESSAGE_SIZE]u8 = undefined;
        const raw_msg = try sock.receive(io, &buf);
        return Message.deserialize(raw_msg.data);
    }
};

fn failure_detector_pinger(io: Io, gpa: Allocator, sock: *const net.Socket) !void {
    // var arena_state = std.heap.ArenaAllocator.init(gpa);
    // const arena = arena_state.allocator();
    var prng = std.Random.DefaultPrng.init(0);
    const rand = prng.random();

    var alive_members = std.ArrayList(*Membership).empty;
    ping_loop: while (true) {
        try io.sleep(NodeStatus.PING_INTERVAL, .awake);
        alive_members.clearRetainingCapacity();
        for (&memberships, 0..) |*member, id|
            if (member.status.load(.monotonic) != .dead and id != self_id) alive_members.append(gpa, member) catch @panic("OOM");
        if (alive_members.items.len == 0)
            continue :ping_loop;

        const ping_target = alive_members.items[rand.uintAtMost(usize, alive_members.items.len-1)];

        ping_target.pending_ack.store(0, .monotonic);
        try sock.send(io, &ping_target.ping_addr, &(Message { .type = .ping, .from = self_id }).serialize());
        _ = ping_target.pending_ack.store(1, .monotonic);

        const start = Io.Timestamp.now(io, .awake);
        while (ping_target.pending_ack.load(.acquire) > 0) {
            const now = Io.Timestamp.now(io, .awake);
            if (start.durationTo(now).nanoseconds > NodeStatus.SUSPECTED_TIMEOUT.nanoseconds) break;
        } else continue :ping_loop;

        ping_target.status.store(.dead, .monotonic);
    }
}

fn failure_detector_server(io: Io, _: Allocator, sock: *const net.Socket) !void {
    while (true) {
        const msg = Message.recv(io, sock)  catch |e| {
            log.err("recv failed: {}", .{e});
            continue;
        };

        const from = &memberships[msg.from];
        switch (msg.type) {
            .ping => {
                sock.send(io, &from.ping_addr, &(Message { .from = self_id, .type = .ack }).serialize()) catch |e| {
                    log.err("sending ack to {f} failed: {}", .{ from.ping_addr, e });
                };
            },
            .ack => {
                _ = from.pending_ack.fetchSub(1, .acquire);
            }
        }
    }
}

pub const PackedMember = extern struct {
    id: Id,
    status: NodeStatus,
};


const MulticastRequest = union(Type) {

    pub const Type = enum(u8) {
        intro,
        member_change,
    };
    intro: Id,
    member_change: PackedMember,

    pub fn deserialize(reader: *Io.Reader) !MulticastRequest {
        const ty = try reader.takeEnum(Type, .native);
        switch (ty) {
            .intro => {
                const id = try reader.takeInt(Id, .native);
                return .{ .intro = id };
            },
            .member_change => {
                const member = try reader.takeStruct(PackedMember, .native);
                return  .{ .member_change = member };
            },
        }
    }

    pub fn serialize(self: MulticastRequest, writer: *Io.Writer) !void {
        try writer.writeByte(@intFromEnum(self));
        switch (self) {
            .intro => |id| {
                try writer.writeInt(Id, id, .native);
            },
            .member_change => |member| {
                try writer.writeStruct(member, .native);
            },
        }
    }
};

const Introduction = struct {
    memberships: []const PackedMember,

    pub fn deserialize(reader: *Io.Reader, arena: Allocator) !Introduction {
        const count = try reader.takeInt(Id, .native);
        const members = arena.alloc(PackedMember, count) catch @panic("OOM");
        for (members) |*member| {
            member.* = try reader.takeStruct(PackedMember, .native);
        }
        return .{ .memberships = members };
    }

    pub fn serialize(self: Introduction, writer: *Io.Writer) !void {
        try writer.writeInt(u32, @intCast(self.memberships.len), .native);
        try writer.writeSliceEndian(PackedMember, self.memberships, .native);
    }
};

fn multicast_server(io: Io, gpa: Allocator, ip_addr: net.IpAddress) void {
    log.info("starting tcp server at {f}", .{ip_addr});
    var server = ip_addr.listen(io, .{ .reuse_address = true }) catch @panic("failed to start multicast server");
    defer server.deinit(io);

    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    while (true) {
        _ = arena_state.reset(.retain_capacity);
        const stream = server.accept(io) catch |e| {
            log.err("introducer accept connection failed: {}", .{e});
            continue;
        };
        defer stream.close(io);
        const req = blk: {
            var reader_buf: [256]u8 = undefined;
            var reader = stream.reader(io, &reader_buf);

            const req = MulticastRequest.deserialize(&reader.interface) catch |e| {
                log.err("failed to read request: {}", .{e});
                continue;
            };
            stream.shutdown(io, .recv) catch unreachable;
            break :blk req;
        };

        switch (req) {
            .intro => |new_id| {
                memberships[new_id].status.store(.alive, .monotonic);

                var packed_members = std.ArrayList(PackedMember).empty;
                for (memberships, 0..) |member, id| {
                    const status = member.status.load(.monotonic);
                    if (status != .dead)
                        packed_members.append(arena, .{ .id = @intCast(id), .status = status }) catch @panic("OOM");
                }
                var writer_buf: [256]u8 = undefined;
                var writer = stream.writer(io, &writer_buf);

                (Introduction {.memberships = packed_members.items}).serialize(&writer.interface) catch |e| {
                    log.err("introducer failed to send introduction: {}", .{e});
                    continue;
                };
                writer.interface.flush() catch |e| log.err("introducer failed to flush: {}", .{e});

                broadcast_change(io, new_id, .alive) catch |e| {
                    log.err("cannot boardcast new member {}: {}", .{ new_id, e });
                    continue;
                };
            },
            .member_change => |member| {
                memberships[member.id].status.store(member.status, .monotonic);
            }
        }
    }

}

fn intro(io: Io, gpa: Allocator, id: Id, ip_addr: net.IpAddress) !void {
    log.info("introducing myself: {}", .{ id });
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const stream = try ip_addr.connect(io, .{ .mode = .stream });
    defer stream.close(io);

    var writer_buf: [256]u8 = undefined;
    var writer = stream.writer(io, &writer_buf);
    var reader_buf: [256]u8 = undefined;
    var reader = stream.reader(io, &reader_buf);

    const req = MulticastRequest { .intro = id };
    try req.serialize(&writer.interface);
    try writer.interface.flush();
    try stream.shutdown(io, .send);

    const introduction = try Introduction.deserialize(&reader.interface, arena);
    for (introduction.memberships) |intro_member| {
        memberships[intro_member.id].status = .init(intro_member.status);
    }
    log.info("introduction done, new members: {}", .{ introduction.memberships.len });
}

fn broadcast_change(io: Io, member_id: Id, status: NodeStatus) !void {
    for (memberships, 0..) |member, id| {
        if (id == self_id or member.status.load(.monotonic) == .dead) continue;
        const stream = member.multicast_addr.connect(io, .{ .mode = .stream }) catch |e| {
            log.err("cannot connect to {f}", .{member.multicast_addr});
            return e;
        };
        defer stream.close(io);

        var writer_buf: [256]u8 = undefined;
        var writer = stream.writer(io, &writer_buf);

        const req = MulticastRequest { .member_change = .{ .id = member_id, .status = status } };
        try req.serialize(&writer.interface);
        try writer.interface.flush();
        try stream.shutdown(io, .send);
    }
}

fn print_color(term: Io.Terminal, color: Io.Terminal.Color, comptime fmt: []const u8, args: anytype) void {
    term.setColor(color) catch unreachable;
    term.writer.print(fmt, args) catch unreachable;
    term.setColor(.reset) catch unreachable;
}

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa = init.gpa;
    const stdout_file = Io.File.stdout();
    try stdout_file.enableAnsiEscapeCodes(io);
    var stdout_buf: [256]u8 = undefined;
    var stdout_writer = stdout_file.writer(io, &stdout_buf);
    const stdout = &stdout_writer.interface;

    const term_mode = try Io.Terminal.Mode.detect(io, stdout_file, false, true);
    const term = Io.Terminal { .mode = term_mode, .writer = stdout };

    var args = init.minimal.args.iterate();
    _ = args.next();
    self_id = try std.fmt.parseInt(u16, args.next().?, 10);
    assert(self_id <  port_list.len);
    log.debug("size of gossip message: {}", .{ @sizeOf(MembershipList) });

    for (port_list, &memberships, 0..) |port, *member, id| {
        member.ping_addr = net.IpAddress.parse("127.0.0.1", port) catch unreachable;
        member.multicast_addr = net.IpAddress.parse("127.0.0.1", port+1) catch unreachable;
        member.status = .init(if (self_id == id) .alive else .dead);
        member.pending_ack = .init(0);
    }

    const self_ping_addr = memberships[self_id].ping_addr;
    const self_multicast_addr = memberships[self_id].multicast_addr;
    const sock = try self_ping_addr.bind(io, .{ .mode = .dgram, .protocol = .udp });
    defer sock.close(io);

    var pinger_fut = try io.concurrent(failure_detector_pinger, .{ io, gpa, &sock });
    var server_fut = try io.concurrent(failure_detector_server, .{ io, gpa, &sock });
    defer { pinger_fut.cancel(io) catch unreachable; server_fut.cancel(io) catch unreachable; }
    _ = try io.concurrent(multicast_server, .{ io, gpa, self_multicast_addr });
    if (self_id != 0)
        intro(io, gpa, self_id, memberships[0].multicast_addr) catch |e| {
            log.err("node failed introduce itself: {}", .{e});
            return e;
        };

    const refresh_rate_ns: Io.Duration = .fromMilliseconds(80);
    while (true) {
        try print_memberships(term, memberships);
        for (0..MAX_NODE+1) |_|
            try stdout.writeAll(Terminal.move_up);
        try stdout_writer.flush();
        try io.sleep(refresh_rate_ns, .awake);
    }
}
