const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const mod = b.addModule("rabbitmq_stream", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const unit_tests = b.addTest(.{ .root_module = mod });
    const run_unit_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);

    const integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/integration.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "rabbitmq_stream", .module = mod }},
        }),
    });
    const run_integration_tests = b.addRunArtifact(integration_tests);
    // The outcome depends on the broker, so never reuse a cached result.
    run_integration_tests.has_side_effects = true;
    const integration_step = b.step("integration", "Run integration tests against a RabbitMQ node");
    integration_step.dependOn(&run_integration_tests.step);

    const examples_step = b.step("examples", "Build the examples");
    inline for (.{ "publish", "consume", "super_stream", "filtering", "perf" }) |name| {
        const exe = b.addExecutable(.{
            .name = name,
            .root_module = b.createModule(.{
                .root_source_file = b.path("examples/" ++ name ++ ".zig"),
                .target = target,
                .optimize = optimize,
                .imports = &.{.{ .name = "rabbitmq_stream", .module = mod }},
            }),
        });
        const install = b.addInstallArtifact(exe, .{});
        examples_step.dependOn(&install.step);
        const run = b.addRunArtifact(exe);
        run.addPassthruArgs();
        b.step("run-" ++ name, "Run the " ++ name ++ " example").dependOn(&run.step);
    }
}
