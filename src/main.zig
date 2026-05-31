const std = @import("std");
const assert = std.debug.assert;
const Io = std.Io;
const Duration = Io.Duration;
const net = Io.net;
const log = std.log;
const fatal = std.process.fatal;
const NetworkConfig = @import("network_config");
const port_list = NetworkConfig.port_list;

const MAX_NODE = port_list.len;
var memberships: MembershipList = undefined;
var ip_addres: [MAX_NODE]net.IpAddress = undefined;
var self_id: Id = undefined;
var self_gen: u32 = 1;

const Id = u32;

const Membership = struct {
    status: NodeStatus,
    gen: u32,
    last_heard: Io.Timestamp,
    
    pub const init = Membership { .status = .dead, .gen =  0, .last_heard = .zero };
};
const MembershipList = [MAX_NODE]Membership;

fn print_memberships(term: Io.Terminal, ships: MembershipList) !void {
    const stdout = term.writer;
    try stdout.print("{s: <10}{s: <10}{s: <10}{s: <10}\n", .{ "id", "status", "time", "gen" });
    for (ships, 0..) |member, id| {
        const id_color: Io.Terminal.Color = if (self_id == id) .blue else .reset;
        print_color(term, id_color, "{: <10}", .{ id });
        const status_color: Io.Terminal.Color = switch (member.status) {
            .alive => .green,
            .suspected => .yellow,
            .dead => .red, 
        };
        print_color(term, status_color, "{s: <10}", .{ @tagName(member.status) });
        try stdout.print("{: <10}{: <10}\n", .{ member.last_heard.toSeconds(), member.gen });
    }
}

const NodeStatus = enum {
    dead,
    suspected,
    alive,

    pub const SUSPECTED_TIMEOUT = Duration.fromMilliseconds(2000);
    pub const DEAD_TIMEOUT = Duration.fromMilliseconds(4000);
    comptime { assert(DEAD_TIMEOUT.nanoseconds > SUSPECTED_TIMEOUT.nanoseconds); }
    pub const GOSSIP_INTERVAL = Duration.fromMilliseconds(500);
    pub const GOSSIP_RECV_TIMEOUT = Duration.fromMilliseconds(500);
};

const Terminal = struct {
    const clear = "\x1b[1J";
    const move_up = "\x1b[1A";
};

fn find_id_by_addr(target: net.IpAddress) Id {
    for (ip_addres, 0..) |addr, id| {
        if (target.eql(&addr)) return @intCast(id);
    } else unreachable;
}

fn failure_detection_recver(io: Io, sock: *const net.Socket) !void {
    while (true) {
        var buf: [@sizeOf(MembershipList)]u8 = undefined;
        const msg_or_timeout = sock.receiveTimeout(io, &buf, .{ .duration = .{ .raw = NodeStatus.GOSSIP_RECV_TIMEOUT, .clock = .awake } });
        const now = Io.Clock.now(.awake, io);
        if (msg_or_timeout) |msg| {
            const other_memberships: * align(1) MembershipList = std.mem.bytesAsValue(MembershipList, msg.data);
            const id = find_id_by_addr(msg.from);
            for (other_memberships, &memberships) |other_member, *member| {
                if (other_member.gen <= member.gen or other_member.status != .alive) {
                    continue;
                }
                member.gen = other_member.gen;
                member.last_heard = now;
                member.status = .alive;
            }
            assert(memberships[id].status == .alive);
        } else |e| {
            if (e != net.Socket.ReceiveTimeoutError.Timeout) return e;
        }

        for (&memberships, 0..) |*member, id| {
            if (id != self_id and member.status != .dead) {
                const timeout = member.last_heard.durationTo(now);
                switch (member.status) {
                    .dead => unreachable,
                    .suspected => { if (timeout.nanoseconds > NodeStatus.DEAD_TIMEOUT.nanoseconds) member.status = .dead; },
                    .alive => { if (timeout.nanoseconds > NodeStatus.SUSPECTED_TIMEOUT.nanoseconds) member.status = .suspected; },
                }
            }
        }
    }
}

fn failure_detection_sender(io: Io, sock: *const net.Socket) !void {
    while (true) {
        const copied_memberships = memberships;
        const bytes = std.mem.asBytes(&copied_memberships);
        for (ip_addres, 0..) |ip_addr, i|
            if (i != self_id) try sock.send(io, &ip_addr, bytes);
        memberships[self_id].gen += 1;
        try io.sleep(NodeStatus.GOSSIP_INTERVAL, .awake);
    }
}

fn print_color(term: Io.Terminal, color: Io.Terminal.Color, comptime fmt: []const u8, args: anytype) void {
    term.setColor(color) catch unreachable;
    term.writer.print(fmt, args) catch unreachable;
    term.setColor(.reset) catch unreachable;
}

pub fn main(init: std.process.Init) !void {
    const io = init.io;
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

    for (port_list, &ip_addres) |port, *addr|
        addr.* = net.IpAddress.parse("127.0.0.1", port) catch unreachable;
    
    const self_addr = ip_addres[self_id];
    const sock = try self_addr.bind(io, .{ .mode = .dgram, .protocol = .udp });
    defer sock.close(io);

    @memset(&memberships, .init);
    memberships[self_id] = .{
        .status = .alive,
        .gen = self_gen,
        .last_heard = Io.Timestamp.now(io, .awake),
    };
    _ = try io.concurrent(failure_detection_recver, .{ io, &sock });
    _ = try io.concurrent(failure_detection_sender, .{ io, &sock });

    const refresh_rate_ns: Io.Duration = .fromMilliseconds(80);
    while (true) {
        try print_memberships(term, memberships);
        for (0..MAX_NODE+1) |_|
            try stdout.writeAll(Terminal.move_up);
        try stdout_writer.flush();
        try io.sleep(refresh_rate_ns, .awake);
    }
}
