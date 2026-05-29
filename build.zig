const std = @import("std");
const net =  std.Io.net;
const Build = std.Build;


pub fn build(b: *Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const main_mod = b.addModule("main", .{
        .optimize = optimize,
        .target = target,
        .root_source_file = b.path("src/main.zig"),
    });
    const opts = b.addOptions();
    opts.addOption([]const u16, "port_list", &.{
        8080, 8081, 8082,
    });
    main_mod.addImport("network_config", opts.createModule());

    const exe = b.addExecutable(.{
        .name = "node",
        .root_module = main_mod,
    });
    b.installArtifact(exe);
}
