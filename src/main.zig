const std = @import("std");
const assert = std.debug.assert;
const Io = std.Io;
const Duration = Io.Duration;
const net = Io.net;
const fatal = std.process.fatal;
const Allocator = std.mem.Allocator;
const NetworkConfig = @import("network_config");

const MembershipList = std.AutoArrayHashMapUnmanaged(Id, Membership);
const Id = net.IpAddress;

var memberships = MembershipList.empty;
var member_mtx = Io.Mutex.init;
const PIGGYBACK_CAPACITY = 2;
var change_index: u32 = 0;
const ChangeItem = struct {
    count: u32,
    change: PackedMember,
};
var changes = std.ArrayList(ChangeItem).empty;

var self_id: Id = undefined;


const Membership = struct {
    ping_addr: net.IpAddress,
    multicast_addr: net.IpAddress,
    status: std.atomic.Value(NodeStatus),
    pending_ack: std.atomic.Value(i8),
    incarnation: u32,
    suspected_start: Io.Timestamp,

    pub fn new(ip: Id, status: NodeStatus) Membership {
        var multicast_addr = ip;
        multicast_addr.setPort(multicast_addr.getPort()+1);
        return .{
            .ping_addr = ip,
            .multicast_addr = multicast_addr,
            .status = .init(status),
            .pending_ack = .init(0),
            .incarnation = 0,
            .suspected_start = .{.nanoseconds = 0},
        };
    }
};

fn print_memberships(io: Io, term: Io.Terminal, ships: MembershipList) !void {
    _ = io;
    const stdout = term.writer;
    try stdout.print("{s: <50}{s: <10}\n", .{ "id", "status" });
    var it = ships.iterator();
    while (it.next()) |entry| {
        const id = entry.key_ptr.*;
        const member = entry.value_ptr;
        const id_color: Io.Terminal.Color = if (std.meta.eql(self_id, id)) .blue else .reset;
        print_color(term, id_color, "{f: <50}", .{ id });
        const status = member.status.load(.monotonic);
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
    pub const PING_TIMEOUT = Duration { .nanoseconds = NETWORK_ROUNDTRIP_TIME.nanoseconds * 1 };
    pub const PING_REQ_TIMEOUT = Duration { .nanoseconds = NETWORK_ROUNDTRIP_TIME.nanoseconds * 2 };
    pub const DEAD_TIMEOUT = Duration { .nanoseconds = NETWORK_ROUNDTRIP_TIME.nanoseconds * 4 };
    comptime { assert(PING_REQ_TIMEOUT.nanoseconds > PING_TIMEOUT.nanoseconds); }
    pub const PING_INTERVAL = Duration.fromMilliseconds(500);
};

const Terminal = struct {
    const clear = "\x1b[1J";
    const move_up = "\x1b[1A";
};

fn choose_k_from_members(comptime T: type, random: std.Random, array: []const T, k: u32, exclude: usize, arena: Allocator) []u32 {
    var allow_indexes = std.ArrayList(u32).empty;
    for (array, 0..) |_, id| {
        if (id == exclude) continue;
        allow_indexes.append(arena, @intCast(id)) catch @panic("OOM");
    }
    random.shuffle(u32, allow_indexes.items);
    return allow_indexes.items[0..@min(k, allow_indexes.items.len)];
}

pub fn get_latest_change(random: std.Random, arena: Allocator) Changes {
    const latest = arena.alloc(PackedMember, @min(PIGGYBACK_CAPACITY, changes.items.len)) catch @panic("OOM");
    const PIGGYBACK_MAX_TIMES: u32 = @ceil(@log(@as(f32, @floatFromInt(memberships.count()))) * 2);
    for (latest) |*el| {
        if (change_index >= changes.items.len) {
            change_index = 0;
            random.shuffle(ChangeItem, changes.items);
        }
        const change_item = &changes.items[change_index];
        el.* = change_item.change;
        change_item.count += 1;
        if (change_item.count >= PIGGYBACK_MAX_TIMES) {
            _ = changes.swapRemove(change_index);
        } else {
            change_index += 1;
        }
    }
    return .{ .memberships = latest };
}

const Message = struct {
    from: Id,
    src: Id,
    to: Id,
    type: Type,

    piggyback: Changes,
    pub const Type = enum(u8) {
        ping,
        ack,
        ping_req,
    };

    pub const MESSAGE_SIZE = @sizeOf(Id) * 3 + @sizeOf(Type) + @sizeOf(u32) + @sizeOf(PackedMember) * PIGGYBACK_CAPACITY;

    pub fn serialize(msg: Message, arena: Allocator) []u8 {
        assert(PIGGYBACK_CAPACITY >= msg.piggyback.memberships.len);
        var buf: [MESSAGE_SIZE]u8 = undefined;
        var writer = Io.Writer.fixed(&buf);
        ip_write(msg.from, &writer) catch unreachable;
        ip_write(msg.src, &writer) catch unreachable;
        ip_write(msg.to, &writer) catch unreachable;
        writer.writeInt(u8, @intFromEnum(msg.type), .little) catch unreachable;
        msg.piggyback.serialize(&writer) catch unreachable;
        return arena.dupe(u8, writer.buffered()) catch @panic("OOM");
    }
    pub fn deserialize(bytes: []const u8, arena: Allocator) !Message {
        var reader = Io.Reader.fixed(bytes);
        const from = try ip_read(&reader);
        const src = try ip_read(&reader);
        const to = try ip_read(&reader);
        const ty = try reader.takeEnum(Type, .little);
        const piggyback = try Changes.deserialize(&reader, arena);
        return .{ .from = from, .src = src, .to = to, .type = ty, .piggyback = piggyback };
    }

    pub fn recv(io: Io, sock: *const net.Socket, arena: Allocator) !struct { net.IpAddress, Message } {
        var buf: [Message.MESSAGE_SIZE]u8 = undefined;
        const raw_msg = try sock.receive(io, &buf);
        return .{ raw_msg.from, try Message.deserialize(raw_msg.data, arena) };
    }
};

fn failure_detector_pinger(io: Io, gpa: Allocator, sock: *const net.Socket) !void {
    const log = std.log.scoped(.pinger);

    var arena_state = std.heap.ArenaAllocator.init(gpa);
    const arena = arena_state.allocator();

    var prng = std.Random.DefaultPrng.init(0);
    const rand = prng.random();

    ping_loop: while (true) {
        try io.sleep(NodeStatus.PING_INTERVAL, .awake);

        _ = arena_state.reset(.retain_capacity);

        var alive_members = std.ArrayList(struct { Id, *Membership }).empty;

        member_mtx.lockUncancelable(io);
        var it = memberships.iterator();
        while  (it.next()) |entry| {
            const id = entry.key_ptr.*;
            const member = entry.value_ptr;
            if (std.meta.eql(id, self_id)) continue;
            switch (member.status.load(.monotonic)) {
                .alive => alive_members.append(arena, .{ id, member }) catch @panic("OOM"),
                .suspected => {
                    if (member.suspected_start.durationTo(Io.Timestamp.now(io, .awake)).nanoseconds > NodeStatus.DEAD_TIMEOUT.nanoseconds) {
                        member.status.store(.dead, .monotonic);
                    }
                },
                .dead => {},
            }
        }
        const piggyback = get_latest_change(rand, arena);
        member_mtx.unlock(io);

        if (alive_members.items.len == 0)
            continue :ping_loop;

        const rand_idx = rand.uintAtMost(usize, alive_members.items.len-1);
        const ping_id, const ping_target = alive_members.items[rand_idx];

        ping_target.pending_ack.store(0, .monotonic);
        try sock.send(io, &ping_target.ping_addr, (Message { .type = .ping, .from = self_id, .src = self_id, .to = ping_id, .piggyback = piggyback }).serialize(arena));
        _ = ping_target.pending_ack.fetchAdd(1, .monotonic);

        const start = Io.Timestamp.now(io, .awake);
        while (ping_target.pending_ack.load(.acquire) > 0) {
            const now = Io.Timestamp.now(io, .awake);
            if (start.durationTo(now).nanoseconds > NodeStatus.PING_TIMEOUT.nanoseconds) break;
        } else continue :ping_loop;

        const PING_REQ_TARGET_COUNT = 2; // `k` in paper

        const ping_req_targets =
            choose_k_from_members(struct { Id, *Membership }, rand, alive_members.items, PING_REQ_TARGET_COUNT, rand_idx, arena);
        // less than k could be returned
        if (ping_req_targets.len > 0) {
            for (ping_req_targets) |ping_req_id| {
                _, const ping_req_target = alive_members.items[@intCast(ping_req_id)];

                ping_req_target.pending_ack.store(0, .monotonic);

                member_mtx.lockUncancelable(io);
                const new_piggyback = get_latest_change(rand, arena);
                member_mtx.unlock(io);

                try sock.send(io, &ping_req_target.ping_addr, (Message { .type = .ping_req, .from = self_id, .src =  self_id, .to = ping_id, .piggyback = new_piggyback }).serialize(arena));
                _ = ping_req_target.pending_ack.fetchAdd(1, .monotonic);
            }

            while (true) {
                const now = Io.Timestamp.now(io, .awake);
                if (start.durationTo(now).nanoseconds > NodeStatus.PING_REQ_TIMEOUT.nanoseconds) break;
                for (ping_req_targets) |ping_req_id| {
                    _, const ping_req_target = alive_members.items[@intCast(ping_req_id)];
                    if (ping_req_target.pending_ack.load(.acquire) <= 0) continue :ping_loop;
                }
            }
        }

        log.debug("cannot ping: {}", .{ping_id});

        member_mtx.lockUncancelable(io);
        changes.append(gpa, .{ .count = 0, .change = .{ .id = ping_id, .status = .dead, .incarnation = ping_target.incarnation }}) catch @panic("OOM");
        ping_target.status.store(.alive, .monotonic);
        ping_target.suspected_start = Io.Timestamp.now(io, .awake);
        member_mtx.unlock(io);
    }
}

fn failure_detector_server(io: Io, gpa: Allocator, sock: *const net.Socket) !void {
    const log = std.log.scoped(.ping_server);

    var arena_state = std.heap.ArenaAllocator.init(gpa);
    const arena = arena_state.allocator();

    var prng = std.Random.DefaultPrng.init(0);
    const rand = prng.random();

    while (true) {
        _ = arena_state.reset(.retain_capacity);
        const ip, const msg = Message.recv(io, sock, arena)  catch |e| {
            log.err("recv failed: {}", .{e});
            continue;
        };

        switch (msg.type) {
            .ping => {
                // event if the ping is not from a known member, we still send ACK back to the sender.
                const addr =
                    if (memberships.getPtr(msg.from)) |from|
                        from.ping_addr
                    else
                        ip;
                sock.send(io, &addr, (Message { .from = self_id, .src = self_id, .to = msg.src, .type = .ack, .piggyback = get_latest_change(rand, arena) }).serialize(arena)) catch |e| {
                    log.err("sending ack to {f} failed: {}", .{ addr, e });
                };
            },
            .ack => {
                const from = memberships.getPtr(msg.from) orelse continue;
                const to = memberships.getPtr(msg.to) orelse continue;
                if (std.meta.eql(msg.to, self_id))
                    _ = from.pending_ack.fetchSub(1, .acquire)
                else
                    sock.send(io, &to.ping_addr, (Message { .from = self_id, .src = msg.src, .to = msg.to, .type = .ack, .piggyback = get_latest_change(rand, arena) }).serialize(arena)) catch |e| {
                        log.err("fowarding ack sourcing from {} to {} failed: {}", .{ msg.src, msg.to, e });
                    };
            },
            .ping_req => {
                const to = memberships.getPtr(msg.to) orelse continue;
                sock.send(io, &to.ping_addr, (Message { .from = self_id, .src = msg.src, .to = msg.to, .type = .ping, .piggyback = get_latest_change(rand, arena) }).serialize(arena)) catch |e| {
                    log.err("fowarding ping sourcing from {} to {} failed: {}", .{ msg.src, msg.to, e });
                };
            },
        }
        for (msg.piggyback.memberships) |change| {
            var has_change = false;
            log.debug("piggyback: {f} -> {}", .{change.id, change.status});
            member_mtx.lockUncancelable(io);
            defer member_mtx.unlock(io);
            switch (change.status) {
                .alive => {
                    const gop = memberships.getOrPut(gpa, change.id) catch @panic("OOM");
                    const member = gop.value_ptr;
                    if (!gop.found_existing) {
                        member.* = .new(change.id, change.status);
                        has_change = true;
                    }
                    else {
                        if (member.status.load(.monotonic) != .dead and change.incarnation > member.incarnation) {
                            member.incarnation = change.incarnation;
                            member.status.store(change.status, .monotonic);
                            has_change = true;
                        }
                    }
                },
                .suspected => {
                    const gop = memberships.getOrPut(gpa, change.id) catch @panic("OOM");
                    const member = gop.value_ptr;
                    if (!gop.found_existing) {
                        member.* = .new(change.id, change.status);
                        has_change = true;
                    }
                    else {
                        const old_status = member.status.load(.monotonic);
                        if (old_status == .suspected
                            and change.incarnation > member.incarnation) {
                            has_change = true;
                            gop.value_ptr.status.store(change.status, .monotonic);
                        }
                        if (old_status == .alive
                            and change.incarnation >= member.incarnation) {
                            has_change = true;
                            gop.value_ptr.status.store(change.status, .monotonic);
                        }
                    }
                },
                .dead => {
                    has_change = memberships.swapRemove(change.id);
                },
            }
            if (has_change) changes.append(gpa, .{ .count = 0, .change = change }) catch @panic("OOM");
        }
    }
}

pub const PackedMember = struct {
    id: Id,
    status: NodeStatus,
    incarnation: u32,
};

pub fn ip_write(ip: net.IpAddress, writer: *Io.Writer) !void {
    try writer.writeByte(@intFromEnum(ip));
    switch (ip) {
        .ip4 => |ipv4| {
            try writer.writeInt(u16, ipv4.port, .little);
            try writer.writeSliceEndian(u8, &ipv4.bytes, .little);
        },
        .ip6 => |ipv6| {
            try writer.writeInt(u16, ipv6.port, .little);
            try writer.writeSliceEndian(u8, &ipv6.bytes, .little);
            try writer.writeInt(u32, ipv6.interface.index, .little);
        }
    }
}

pub fn ip_read(reader: *Io.Reader) !net.IpAddress {
    const tag_raw = try reader.takeByte();
    const tag = std.enums.fromInt(net.IpAddress.Family, tag_raw) orelse return error.CorruptedIpFamily;
    switch (tag) {
        .ip4 => {
            const port = try reader.takeInt(u16, .little);
            var bytes: [4]u8 = undefined;
            try reader.readSliceEndian(u8, &bytes, .little);
            return .{ .ip4 = .{ .port = port, .bytes = bytes } };
        },
        .ip6 => {
            const port = try reader.takeInt(u16, .little);
            var bytes: [16]u8 = undefined;
            try reader.readSliceEndian(u8, &bytes, .little);
            const interface = try reader.takeInt(u32, .little);
            return .{ .ip6 = .{ .port = port, .bytes = bytes, .interface = .{ .index = interface } } };
        }
    }
}


pub const MulticastRequest = union(Type) {

    pub const Type = enum(u8) {
        intro,
        hello,
    };
    intro: Id, // port number of ping_address
    hello,

    pub fn deserialize(reader: *Io.Reader) !MulticastRequest {
        const ty = try reader.takeEnum(Type, .little);
        switch (ty) {
            .intro => {
                const ip = try ip_read(reader);
                return .{ .intro = ip };
            },
            .hello => return .hello,
        }
    }

    pub fn serialize(self: MulticastRequest, writer: *Io.Writer) !void {
        try writer.writeByte(@intFromEnum(self));
        switch (self) {
            .intro => |ip| {
                try ip_write(ip, writer);
            },
            .hello => {},
        }
    }
};

pub const Changes = struct {
    memberships: []const PackedMember,

    pub fn deserialize(reader: *Io.Reader, arena: Allocator) !Changes {
        const count = try reader.takeInt(u32, .little);
        const members = arena.alloc(PackedMember, count) catch @panic("OOM");
        for (members) |*member| {
            member.incarnation = try reader.takeInt(u32, .little);
            member.status = try reader.takeEnum(NodeStatus, .little);
            member.id = try ip_read(reader);
        }
        return .{ .memberships = members };
    }

    pub fn serialize(self: Changes, writer: *Io.Writer) !void {
        try writer.writeInt(u32, @intCast(self.memberships.len), .little);
        for (self.memberships) |member| {
            try writer.writeInt(u32, member.incarnation, .little);
            try writer.writeByte(@intFromEnum(member.status));
            try ip_write(member.id, writer);
        }
    }
};

pub const MONITOR_PORT = 3000;

fn multicast_server(io: Io, gpa: Allocator, ip_addr: net.IpAddress) void {
    const log = std.log.scoped(.multicast_server);
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

        var writer_buf: [256]u8 = undefined;
        var writer = stream.writer(io, &writer_buf);
        defer writer.interface.flush() catch |e| log.err("introducer failed to flush: {}", .{e});
        switch (req) {
            .intro => |ping_port| {
                member_mtx.lockUncancelable(io);
                memberships.put(gpa, ping_port, .new(ping_port, .alive)) catch @panic("OOM");

                var packed_members = std.ArrayList(PackedMember).empty;
                var it = memberships.iterator();
                while (it.next()) |entry| {
                    const id = entry.key_ptr.*;
                    const member = entry.value_ptr;
                    const status = member.status.load(.monotonic);
                    if (status != .dead)
                        packed_members.append(arena, .{ .id = id, .status = status, .incarnation = 0 }) catch @panic("OOM");
                }
                if (ping_port.getPort() != MONITOR_PORT) {
                    changes.append(gpa, .{ .count = 0, .change = .{ .id = ping_port, .status = .alive, .incarnation = 0 } }) catch @panic("OOM");
                }
                member_mtx.unlock(io);

                (Changes {.memberships = packed_members.items}).serialize(&writer.interface) catch |e| {
                    log.err("introducer failed to send introduction: {}", .{e});
                    continue;
                };
            },
            .hello => {
                writer.interface.writeByte(69) catch continue;
            },
        }
    }

}

fn intro(io: Io, gpa: Allocator, id: Id, ip_addr: net.IpAddress) !void {
    std.log.info("introducing myself: {}", .{ id });
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

    const introduction = try Changes.deserialize(&reader.interface, arena);
    std.log.info("introduction, new members: {}", .{ introduction.memberships.len });
    try member_mtx.lock(io);
    for (introduction.memberships) |intro_member| {
        memberships.put(gpa, intro_member.id, .new(intro_member.id, intro_member.status)) catch @panic("OOM");
    }
    member_mtx.unlock(io);
}

fn print_color(term: Io.Terminal, color: Io.Terminal.Color, comptime fmt: []const u8, args: anytype) void {
    term.setColor(color) catch unreachable;
    term.writer.print(fmt, args) catch unreachable;
    term.setColor(.reset) catch unreachable;
}

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa = init.gpa;

    // const stdout_file = Io.File.stderr();
    // try stdout_file.enableAnsiEscapeCodes(io);
    // var stdout_buf: [256]u8 = undefined;
    // var stdout_writer = stdout_file.writer(io, &stdout_buf);
    // const stdout = &stdout_writer.interface;

    // const term_mode = try Io.Terminal.Mode.detect(io, stdout_file, false, true);
    // const term = Io.Terminal { .mode = term_mode, .writer = stdout };

    var args = init.minimal.args.iterate();
    _ = args.next().?;
    const ping_port_str = args.next().?;
    const ping_port = try std.fmt.parseInt(u16, ping_port_str, 10);
    const multicast_port = ping_port+1;
    std.log.debug("size of gossip message: {}", .{ Message.MESSAGE_SIZE });
    std.log.debug("ping port: {}, multicast port: {}", .{ ping_port, multicast_port });

    self_id = try net.IpAddress.parse("127.0.0.1", ping_port);
    const self = Membership {
        .status = .init(.alive),
        .ping_addr = self_id,
        .multicast_addr = try net.IpAddress.parse("127.0.0.1", multicast_port),
        .pending_ack = .init(0),
        .incarnation = 0,
        .suspected_start = .{.nanoseconds=0}
    };
    memberships.put(gpa, self_id, self) catch @panic("OOM");

    const sock = try self.ping_addr.bind(io, .{ .mode = .dgram, .protocol = .udp });
    defer sock.close(io);

    var pinger_fut = try io.concurrent(failure_detector_pinger, .{ io, gpa, &sock });
    var server_fut = try io.concurrent(failure_detector_server, .{ io, gpa, &sock });
    defer { pinger_fut.cancel(io) catch unreachable; server_fut.cancel(io) catch unreachable; }
    _ = try io.concurrent(multicast_server, .{ io, gpa, self.multicast_addr });
    if (self_id.getPort() != 8000) {
        const known_introducer_port = 8001;
        const introducer_addr = try net.IpAddress.parse("127.0.0.1", known_introducer_port);
        intro(io, gpa, self_id, introducer_addr) catch |e| {
            std.log.err("node failed introduce itself: {}", .{e});
            return e;
        };
    }

    const refresh_rate_ns: Io.Duration = .fromMilliseconds(80);
    var last_len: usize = 0;
    var buf: [256]u8 = undefined;
    while (true) {
        const stderr = std.debug.lockStderr(&buf);
        const term = stderr.terminal();
        for (0..last_len) |_|
            try term.writer.writeAll(Terminal.move_up);
        try print_memberships(io, term, memberships);
        last_len = memberships.count() + 1;
        std.debug.unlockStderr();
        try io.sleep(refresh_rate_ns, .awake);
    }
}

const testing = std.testing;

test Message {
    const gpa = testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();

    const id1 = try net.IpAddress.parse("192.168.1.1", 4200);
    const id2 = try net.IpAddress.parse("2001:0db8:85a3:0000:8a2e:0370:7334:1234", 1234);

    const piggyback = Changes { .memberships = &.{ .{ .id = id1, .status = .alive, .incarnation = 101, }, .{ .id = id2, .status = .suspected, .incarnation = 250 } } };
    const msg1 = Message { .src = id1, .from = id1, .to = id2, .type = .ping, .piggyback = piggyback };
    const buf = msg1.serialize(arena_state.allocator());
    const msg2 = Message.deserialize(buf, arena_state.allocator());

    try testing.expectEqualDeep(msg1, msg2);
}

test MulticastRequest {
    const gpa = testing.allocator;
    const id1 = try net.IpAddress.parse("192.168.1.1", 4200);
    const req1 = MulticastRequest { .intro = id1 };
    var writer = Io.Writer.Allocating.init(gpa);
    defer writer.deinit();
    try req1.serialize(&writer.writer);

    var reader = Io.Reader.fixed(writer.written());
    const req2 = try MulticastRequest.deserialize(&reader);

    try testing.expectEqual(req1, req2);
}

test Changes {
    const gpa = testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const id1 = try net.IpAddress.parse("192.168.1.1", 4200);
    const id2 = try net.IpAddress.parse("2001:0db8:85a3:0000:8a2e:0370:7334:1234", 1234);
    const in1 = Changes { .memberships = &.{ .{ .id = id1, .status = .alive, .incarnation = 0,}, .{ .id = id2, .status = .suspected, .incarnation = 365 } }};
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);
    try in1.serialize(&writer);

    var reader = Io.Reader.fixed(writer.buffered());
    const in2 = Changes.deserialize(&reader, arena_state.allocator());

    try testing.expectEqualDeep(in1, in2);
}

test "IpAddress Read/Write" {
    const ip1 = try net.IpAddress.parse("127.0.0.1", 8000);
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);
    try ip_write(ip1, &writer);

    var reader = Io.Reader.fixed(writer.buffered());
    const ip1_ = try ip_read(&reader);

    try testing.expectEqualDeep(ip1, ip1_);

    const ip2 = try net.IpAddress.parse("::", 6969);
    writer = Io.Writer.fixed(&buf);
    try ip_write(ip2, &writer);

    reader = Io.Reader.fixed(writer.buffered());
    const ip2_ = try ip_read(&reader);

    try testing.expectEqualDeep(ip2, ip2_);
}
