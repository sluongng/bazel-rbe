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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.devtools.build.lib.platform.PlatformNativeDepsService;
import com.google.devtools.build.lib.profiler.SystemNetworkStatsService;
import com.google.devtools.build.lib.runtime.BlazeService;
import com.google.devtools.build.lib.skyframe.FsEventsNativeDepsService;
import com.google.devtools.build.lib.unix.ProcessUtilsService;
import com.google.devtools.common.options.OptionsProvider;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.IntConsumer;

/** Services that are used in Bazel */
@SuppressWarnings("UnnecessarilyFullyQualified") // Class names fully qualified for clarity.
public final class BazelServices {

  public static final ImmutableList<BlazeService> BAZEL_SERVICES =
      isNativeImage() ? nativeImageServices() : jvmServices();

  private static ImmutableList<BlazeService> jvmServices() {
    return
      ImmutableList.of(
          new com.google.devtools.build.lib.skyframe.FsEventsNativeDepsServiceImpl(),
          new com.google.devtools.build.lib.platform.PlatformNativeDepsServiceImpl(),
          new com.google.devtools.build.lib.profiler.SystemNetworkStatsServiceImpl(),
          new com.google.devtools.build.lib.profiler.TraceProfilerServiceImpl(),
          new com.google.devtools.build.lib.unix.NativePosixFilesServiceImpl(),
          new com.google.devtools.build.lib.unix.ProcessUtilsServiceImpl(),
          new com.google.devtools.build.lib.server.GrpcCommandServerServiceImpl(),
          new com.google.devtools.build.lib.starlarkprofiler.CpuProfilerServiceImpl(),
          new com.google.devtools.build.lib.util.ServerLogPathServiceImpl());
  }

  private static ImmutableList<BlazeService> nativeImageServices() {
    return ImmutableList.of(
        new NativeImageFsEventsNativeDepsService(),
        new NativeImagePlatformNativeDepsService(),
        new NativeImageSystemNetworkStatsService(),
        new com.google.devtools.build.lib.profiler.TraceProfilerServiceImpl(),
        new com.google.devtools.build.lib.unix.NativePosixFilesServiceImpl(),
        new NativeImageProcessUtilsService(),
        new com.google.devtools.build.lib.server.GrpcCommandServerServiceImpl(),
        new com.google.devtools.build.lib.starlarkprofiler.CpuProfilerServiceImpl(),
        new com.google.devtools.build.lib.util.ServerLogPathServiceImpl());
  }

  private static boolean isNativeImage() {
    return System.getProperty("org.graalvm.nativeimage.imagecode") != null;
  }

  private static final class NativeImageFsEventsNativeDepsService
      implements FsEventsNativeDepsService {
    @Override
    public void createFsEvents(byte[][] paths, byte[][] excludedPaths, double latency) {}

    @Override
    public void runFsEvents(CountDownLatch listening) {
      listening.countDown();
    }

    @Override
    public void doCloseFsEvents() {}

    @Override
    public byte[][] pollFsEvents() {
      return null;
    }
  }

  private static final class NativeImagePlatformNativeDepsService
      implements PlatformNativeDepsService {
    @Override
    public int pushDisableSleep() {
      return -1;
    }

    @Override
    public int popDisableSleep() {
      return -1;
    }

    @Override
    public void registerCPUSpeedJni(IntConsumer callback) {}

    @Override
    public int cpuSpeed() {
      return -1;
    }

    @Override
    public void registerDiskSpaceJni(IntConsumer callback) {}

    @Override
    public void registerLoadAdvisoryJni(IntConsumer callback) {}

    @Override
    public int systemLoadAdvisory() {
      return -1;
    }

    @Override
    public void registerMemoryPressureJni(IntConsumer callback) {}

    @Override
    public int systemMemoryPressure() {
      return -1;
    }

    @Override
    public void registerSuspensionJni(IntConsumer callback) {}

    @Override
    public void registerThermalJni(IntConsumer callback) {}

    @Override
    public int thermalLoad() {
      return -1;
    }
  }

  private static final class NativeImageSystemNetworkStatsService
      implements SystemNetworkStatsService {
    @Override
    public Map<String, NetIoCounter> getNetIoCounters() throws IOException {
      return ImmutableMap.of();
    }
  }

  private static final class NativeImageProcessUtilsService implements ProcessUtilsService {
    @Override
    public void globalInit(OptionsProvider startupOptions, Iterable<BlazeService> blazeServices) {
      ProcessUtilsService.registerJniService(this);
    }

    @Override
    public int getgid() {
      return getProcSelfId("gid");
    }

    @Override
    public int getuid() {
      return getProcSelfId("uid");
    }

    private static int getProcSelfId(String attribute) {
      try {
        return ((Number) Files.getAttribute(Path.of("/proc/self"), "unix:" + attribute))
            .intValue();
      } catch (IOException | UnsupportedOperationException e) {
        throw new UnsupportedOperationException(e);
      }
    }
  }

  private BazelServices() {}
}
