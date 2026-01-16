// Copyright 2025 The Bazel Authors. All rights reserved.
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
package com.google.devtools.build.lib.skyframe.serialization;

import com.google.common.io.BaseEncoding;
import com.google.devtools.build.lib.skyframe.serialization.analysis.ClientId.LongVersionClientId;
import com.google.devtools.build.lib.skyframe.serialization.analysis.ClientId.SnapshotClientId;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisCacheKey;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisCacheLookupRequest;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.ClientId;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.FrontierVersion;
import com.google.devtools.build.skyframe.SkyKey;

/** Utility for building analysis cache lookup protos. */
public final class AnalysisCacheKeyProtoUtils {
  private AnalysisCacheKeyProtoUtils() {}

  public static AnalysisCacheLookupRequest buildLookupRequest(
      PackedFingerprint fingerprint, SkyKey key, FrontierNodeVersion version) {
    return AnalysisCacheLookupRequest.newBuilder()
        .setKey(buildKey(fingerprint, key, version))
        .build();
  }

  public static AnalysisCacheKey buildKey(
      PackedFingerprint fingerprint, SkyKey key, FrontierNodeVersion version) {
    return AnalysisCacheKey.newBuilder()
        .setFingerprintHex(toHex(fingerprint.toBytes()))
        .setFrontierVersion(buildFrontierVersion(version))
        .setSkyKey(key.toString())
        .setSkyFunction(key.functionName().getName())
        .build();
  }

  public static FrontierVersion buildFrontierVersion(FrontierNodeVersion version) {
    FrontierVersion.Builder builder =
        FrontierVersion.newBuilder()
            .setTopLevelConfigChecksum(version.getTopLevelConfigChecksum())
            .setBlazeInstallMd5(version.getBlazeInstallMD5().toString())
            .setEvaluatingVersion(version.getEvaluatingVersion())
            .setUseFakeStampData(version.getUseFakeStampData());
    version.getClientId().ifPresent(clientId -> builder.setClientId(toProtoClientId(clientId)));
    return builder.build();
  }

  private static ClientId toProtoClientId(
      com.google.devtools.build.lib.skyframe.serialization.analysis.ClientId clientId) {
    return switch (clientId) {
      case SnapshotClientId snapshot ->
          ClientId.newBuilder()
              .setSnapshot(
                  com.google.devtools.build.lib.skyframe.serialization.analysis.proto
                      .SnapshotClientId.newBuilder()
                      .setWorkspaceId(snapshot.workspaceId())
                      .setSnapshotVersion(snapshot.snapshotVersion())
                      .build())
              .build();
      case LongVersionClientId longVersion ->
          ClientId.newBuilder().setEvaluatingVersion(longVersion.evaluatingVersion()).build();
    };
  }

  private static String toHex(byte[] bytes) {
    return BaseEncoding.base16().lowerCase().encode(bytes);
  }
}
