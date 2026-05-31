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

    while (true) {
        try stdout.print("monitor> ", .{}); try stdout.flush();
        const line = try stdin.takeDelimiter('\n') orelse continue;
        var tks = std.mem.tokenizeScalar(u8, line, ' ');
        const command = tks.next() orelse continue;
        if (std.mem.eql(u8, command, "member")) {
           try stdout.print("memberships:\n", .{});
        } else {
            try stdout.print("unknown command: {s}\n", .{command});
        }
    }

}

const std = @import("std");
const Io = std.Io;
