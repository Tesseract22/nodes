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
        8000, 8010, 8020, 8030, 8040, 8050, 8060, 8070, 8080, 8090,
    });
    main_mod.addImport("network_config", opts.createModule());

    const monitor_mod = b.addModule("monitor", .{
        .optimize = optimize,
        .target = target,
        .root_source_file = b.path("src/monitor.zig"),
    });

    const node = b.addExecutable(.{
        .name = "node",
        .root_module = main_mod,
    });
    b.installArtifact(node);

    const monitor = b.addExecutable(.{
        .name = "monitor",
        .root_module = monitor_mod,
    });
    b.installArtifact(monitor);

}
