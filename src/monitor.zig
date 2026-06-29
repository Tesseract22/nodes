//! This file implements a monitor that is able to query status for a running node

pub fn main(init: std.process.Init) !void {
    const io = init.io;

    const stdout_file = Io.File.stdout();
    try stdout_file.enableAnsiEscapeCodes(io);
    var stdout_buf: [256]u8 = undefined;
    var stdout_writer = stdout_file.writer(io, &stdout_buf);
    const stdout = &stdout_writer.interface;
    defer stdout.flush() catch unreachable;

    const stdin_file = Io.File.stdin();
    var stdin_buf: [256]u8 = undefined;
    var stdin_reader = stdin_file.reader(io, &stdin_buf);
    const stdin = &stdin_reader.interface;

    try stdout.print("Monitor starting..\n", .{});

    const gpa = init.gpa;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    while (true) {
        _ = arena_state.reset(.retain_capacity);
        try stdout.print("monitor> ", .{}); try stdout.flush();
        const line = try stdin.takeDelimiter('\n') orelse continue;
        var tks = std.mem.tokenizeScalar(u8, line, ' ');
        const command = tks.next() orelse continue;
        if (std.mem.eql(u8, command, "TODO")) {
            try stdout.print("memberships:\n", .{});
        } else if (std.mem.eql(u8, command, "hello")) {
            const port_str = tks.next() orelse {
                try stdout.print("expect <port>\n", .{});
                continue;
            };
            const port = std.fmt.parseInt(u16, port_str, 10) catch |e| {
                try stdout.print("cannot parse <port>: {}\n", .{e});
                continue;
            };
            const ip = net.IpAddress.parse("127.0.0.1", port) catch unreachable;
            const stream = ip.connect(io, .{ .mode = .stream }) catch |e| {
                try stdout.print("cannot connect to {f}: {}\n", .{ip, e});
                continue;
            };
            var writer_buf: [64]u8 = undefined;
            var reader_buf: [64]u8 = undefined;
            var writer = stream.writer(io, &writer_buf);
            var reader = stream.reader(io, &reader_buf);
            const req = FailureDector.MulticastRequest { .hello = {} };
            try req.serialize(&writer.interface);
            try writer.interface.flush();

            const byte = try reader.interface.takeByte();
            try stdout.print("{s}\n", .{if (byte == 69) "success" else "failed"});
        } else if (std.mem.eql(u8, command, "member")) {
            const port_str = tks.next() orelse {
                try stdout.print("expect <port>\n", .{});
                continue;
            };
            const port = std.fmt.parseInt(u16, port_str, 10) catch |e| {
                try stdout.print("cannot parse <port>: {}\n", .{e});
                continue;
            };
            const ip = net.IpAddress.parse("127.0.0.1", port) catch unreachable;
            log.info("ready to connect to {f}", .{ip});
            const stream = ip.connect(io, .{ .mode = .stream }) catch |e| {
                try stdout.print("cannot connect to {f}: {}\n", .{ip, e});
                continue;
            };
            log.info("connected", .{});
            var writer_buf: [64]u8 = undefined;
            var reader_buf: [64]u8 = undefined;
            var writer = stream.writer(io, &writer_buf);
            var reader = stream.reader(io, &reader_buf);
            const req = FailureDector.MulticastRequest { .intro = FailureDector.MONITOR_PORT };
            log.info("intro sent", .{});
            req.serialize(&writer.interface) catch |e| {
                try stdout.print("cannot send request: {}\n", .{e});
                continue;
            };
            try writer.interface.flush();
            const introduction = FailureDector.Changes.deserialize(&reader.interface, arena) catch |e| {
                try stdout.print("cannot read membership list: {}\n", .{e});
                continue;
            };
            try stdout.print("memberships:\n", .{});
            for (introduction.memberships) |member| {
                try stdout.print("{}: {}: {}\n", .{member.id, member.status, member.incarnation});
            }

        } else if (std.mem.eql(u8, command, "udp")) {
            // const port_str = tks.next() orelse {
            //     try stdout.print("expect <port>\n", .{});
            //     continue;
            // };
            // const port = std.fmt.parseInt(u16, port_str, 10) catch |e| {
            //     try stdout.print("cannot parse <port>: {}\n", .{e});
            //     continue;
            // };
            // const self_ip = net.IpAddress.parse("127.0.0.1", 3000) catch unreachable;
            // const ip = net.IpAddress.parse("127.0.0.1", port) catch unreachable;
            // const sock = self_ip.bind(io, .{ .mode = .dgram, .protocol = .udp }) catch |e| {
            //     try stdout.print("cannot bind {f}: {}", .{ self_ip, e });
            //     continue;
            // };

            // const msg = FailureDector.Message { .type = .ping, .from = 69*420, .src = 69*420, .to = ping_id, .piggyback = piggyback };
            // try sock.send(io, &ip, msg.serialize(arena));
        } else {
            try stdout.print("unknown command: {s}\n", .{command});
        }
    }
}

const std = @import("std");
const Io = std.Io;
const net = Io.net;
const log = std.log;
const FailureDector = @import("main.zig");
