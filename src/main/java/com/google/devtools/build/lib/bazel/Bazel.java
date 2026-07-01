// Copyright 2014 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.google.devtools.build.lib.bazel;

import static com.google.devtools.build.lib.bazel.BazelServices.BAZEL_SERVICES;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.devtools.build.lib.analysis.BlazeVersionInfo;
import com.google.devtools.build.lib.authandtls.credentialhelper.CredentialModule;
import com.google.devtools.build.lib.jni.JniLoader;
import com.google.devtools.build.lib.runtime.BlazeModule;
import com.google.devtools.build.lib.runtime.BlazeRuntime;
import com.google.devtools.build.lib.shell.WindowsSubprocessFactory;
import com.google.devtools.build.lib.util.SimpleLogHandler;
import com.google.devtools.build.lib.util.SingleLineFormatter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

/** The main class. */
public final class Bazel {
  private static final String BUILD_DATA_PROPERTIES = "/build-data.properties";
  private static final String BAZEL_NATIVE_FILE_ENCODING = "ISO-8859-1";
  private static final String BAZEL_NATIVE_PLATFORM_ENCODING = "ISO-8859-1";
  private static final String INSTALL_BASE_PROPERTY = "bazel.native.install_base";
  private static final String LOG_HANDLER_QUERIER_PROPERTY =
      "com.google.devtools.build.lib.util.LogHandlerQuerier.class";
  private static final int LOG_ROTATE_LIMIT_BYTES = 1024000;
  private static final int LOG_TOTAL_LIMIT_BYTES = 20 * 1024 * 1024;

  /**
   * The list of modules to load. Note that the order is important: In case multiple modules provide
   * strategies for the same things, the last module wins and its strategy becomes the default.
   *
   * <p>Example: To make the "standalone" execution strategy the default for spawns, put it after
   * all the other modules that provider spawn strategies (e.g. WorkerModule and SandboxModule).
   */
  @SuppressWarnings("UnnecessarilyFullyQualified") // Class names fully qualified for clarity.
  public static final ImmutableList<Class<? extends BlazeModule>> BAZEL_MODULES =
      ImmutableList.of(
          BazelStartupOptionsModule.class,
          // This module is registered early so that profiles are as complete as possible.
          com.google.devtools.build.lib.profiler.CommandProfilerModule.class,
          com.google.devtools.build.lib.starlarkprofiler.CpuProfilerModule.class,
          // This module needs to be registered before any module providing a SpawnCache
          // implementation.
          com.google.devtools.build.lib.runtime.NoSpawnCacheModule.class,
          // This module needs to be registered before any module that uses the credential cache.
          CredentialModule.class,
          com.google.devtools.build.lib.runtime.CommandLogModule.class,
          com.google.devtools.build.lib.runtime.MemoryPressureModule.class,
          com.google.devtools.build.lib.runtime.ThreadDumpModule.class,
          com.google.devtools.build.lib.platform.SleepPreventionModule.class,
          com.google.devtools.build.lib.platform.SystemSuspensionModule.class,
          BazelFileSystemModule.class,
          com.google.devtools.build.lib.runtime.mobileinstall.MobileInstallModule.class,
          com.google.devtools.build.lib.bazel.BazelWorkspaceStatusModule.class,
          com.google.devtools.build.lib.bazel.BazelDiffAwarenessModule.class,
          com.google.devtools.build.lib.remote.RemoteModule.class,
          com.google.devtools.build.lib.bazel.BazelRepositoryModule.class,
          com.google.devtools.build.lib.bazel.repository.starlark.StarlarkRepositoryDebugModule
              .class,
          com.google.devtools.build.lib.bazel.debug.WorkspaceRuleModule.class,
          com.google.devtools.build.lib.bazel.coverage.BazelCoverageReportModule.class,
          com.google.devtools.build.lib.starlarkdebug.module.StarlarkDebuggerModule.class,
          CacheHitReportingModule.class,
          com.google.devtools.build.lib.bazel.SpawnLogModule.class,
          com.google.devtools.build.lib.bazel.bzlmod.BazelLockFileModule.class,
          com.google.devtools.build.lib.outputfilter.OutputFilteringModule.class,
          com.google.devtools.build.lib.worker.WorkerModule.class,
          com.google.devtools.build.lib.runtime.CacheFileDigestsModule.class,
          com.google.devtools.build.lib.standalone.StandaloneModule.class,
          com.google.devtools.build.lib.sandbox.SandboxModule.class,
          com.google.devtools.build.lib.runtime.BuildSummaryStatsModule.class,
          com.google.devtools.build.lib.dynamic.DynamicExecutionModule.class,
          com.google.devtools.build.lib.bazel.rules.BazelRulesModule.class,
          com.google.devtools.build.lib.bazel.rules.BazelStrategyModule.class,
          com.google.devtools.build.lib.network.NoOpConnectivityModule.class,
          com.google.devtools.build.lib.profiler.memory.AllocationTrackerModule.class,
          com.google.devtools.build.lib.packages.metrics.PackageMetricsModule.class,
          com.google.devtools.build.lib.runtime.ExecutionGraphModule.class,
          BazelBuiltinCommandModule.class,
          com.google.devtools.build.lib.includescanning.IncludeScanningModule.class,
          com.google.devtools.build.lib.skyframe.SkymeldModule.class,
          com.google.devtools.build.lib.skyframe.serialization.SerializationModule.class,
          // This module needs to be registered after any module submitting tasks with its {@code
          // submit} method.
          com.google.devtools.build.lib.runtime.BlockWaitingModule.class,
          // This module needs to come after BlockWaitingModule so that the BES isn't closed until
          // the background tasks maintained by the module have completed.
          com.google.devtools.build.lib.buildeventservice.BazelBuildEventServiceModule.class,
          // Modules that are involved in the collection of heap-related metrics of a build. They
          // need to be
          // last in the modules order, so when the GCs happen at the end of the build, we mitigate
          // the risk
          // that objects are still held onto by the other modules. This is a quick fix for
          // b/247613138.
          // TODO(b/253394502): remove this when we have a better solution.
          com.google.devtools.build.lib.metrics.PostGCMemoryUseRecorder
              .PostGCMemoryUseRecorderModule.class,
          com.google.devtools.build.lib.metrics.PostGCMemoryUseRecorder.GcAfterBuildModule.class,
          com.google.devtools.build.lib.metrics.MetricsModule.class);

  public static void main(String[] args) {
    // Sets the default subprocess factory to the Windows-specific implementation if the host OS is
    // Windows. We do this in Bazel.java to make sure that the global state is set before the first
    // use of SubprocessBuilder.
    WindowsSubprocessFactory.maybeInstallWindowsSubprocessFactory();
    configureNativeImageLogging(args);
    BlazeVersionInfo.setBuildInfo(tryGetBuildInfo());
    BlazeRuntime.main(BAZEL_MODULES, BAZEL_SERVICES, args, JniLoader.getJniLoadError());
  }

  private static void configureNativeImageLogging(String[] args) {
    if (!isNativeImage()) {
      return;
    }
    System.setProperty("file.encoding", BAZEL_NATIVE_FILE_ENCODING);
    System.setProperty("native.encoding", BAZEL_NATIVE_PLATFORM_ENCODING);
    System.setProperty("sun.jnu.encoding", BAZEL_NATIVE_PLATFORM_ENCODING);
    Optional<Path> outputBase = getOutputBase(args);
    if (outputBase.isEmpty()) {
      return;
    }
    getInstallBase(args)
        .ifPresent(path -> System.setProperty(INSTALL_BASE_PROPERTY, path.toString()));
    try {
      Files.createDirectories(outputBase.get());
      System.setProperty(
          LOG_HANDLER_QUERIER_PROPERTY, SimpleLogHandler.HandlerQuerier.class.getName());
      Logger rootLogger = Logger.getLogger("");
      for (Handler handler : rootLogger.getHandlers()) {
        rootLogger.removeHandler(handler);
        handler.close();
      }
      rootLogger.setUseParentHandlers(false);
      rootLogger.addHandler(
          SimpleLogHandler.builder()
              .setPrefix(outputBase.get().resolve("java.log").toString())
              .setRotateLimitBytes(LOG_ROTATE_LIMIT_BYTES)
              .setTotalLimitBytes(LOG_TOTAL_LIMIT_BYTES)
              .setFormatter(new SingleLineFormatter())
              .setLogLevel(Level.INFO)
              .build());
      rootLogger.setLevel(Level.INFO);
      Logger.getLogger(Bazel.class.getName()).info("Bazel native-image server logging initialized");
    } catch (IOException | RuntimeException e) {
      System.err.println("Failed to configure native-image server logging: " + e.getMessage());
    }
  }

  private static Optional<Path> getOutputBase(String[] args) {
    return getStartupPath(args, "--output_base");
  }

  private static Optional<Path> getInstallBase(String[] args) {
    return getStartupPath(args, "--install_base");
  }

  private static Optional<Path> getStartupPath(String[] args, String optionName) {
    for (int i = 0; i < args.length; i++) {
      if (args[i].startsWith(optionName + "=")) {
        return getPath(args[i].substring(optionName.length() + 1));
      }
      if (args[i].equals(optionName) && i + 1 < args.length) {
        return getPath(args[i + 1]);
      }
    }
    return Optional.empty();
  }

  private static Optional<Path> getPath(String path) {
    try {
      return Optional.of(Path.of(path));
    } catch (InvalidPathException e) {
      return Optional.empty();
    }
  }

  private static boolean isNativeImage() {
    return System.getProperty("org.graalvm.nativeimage.imagecode") != null;
  }

  /**
   * Builds the standard build info map from the loaded properties. The returned value is the list
   * of "build.*" properties from the build-data.properties file. The final key is the original one
   * striped, dot replaced with a space and with first letter capitalized. If the file fails to load
   * the returned map is empty.
   */
  private static ImmutableMap<String, String> tryGetBuildInfo() {
    try (InputStream in = Bazel.class.getResourceAsStream(BUILD_DATA_PROPERTIES)) {
      if (in == null) {
        return ImmutableMap.of();
      }
      Properties props = new Properties();
      props.load(in);
      ImmutableMap.Builder<String, String> buildData = ImmutableMap.builder();
      for (Object key : props.keySet()) {
        String stringKey = key.toString();
        if (stringKey.startsWith("build.")) {
          // build.label -> Build label, build.timestamp.as.int -> Build timestamp as int
          String buildDataKey = "B" + stringKey.substring(1).replace('.', ' ');
          buildData.put(buildDataKey, props.getProperty(stringKey, ""));
        }
      }
      return buildData.buildOrThrow();
    } catch (IOException ignored) {
      return ImmutableMap.of();
    }
  }

  private Bazel() {}
}
