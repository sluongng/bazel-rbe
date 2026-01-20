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
package com.google.devtools.build.lib.skyframe.serialization.analysis;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.common.base.Strings;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisCacheServiceGrpc;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.BuildMetadata;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.WriteTopLevelTargetsRequest;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.WriteTopLevelTargetsResponse;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import javax.annotation.Nullable;

/** gRPC implementation of {@link RemoteAnalysisMetadataWriter}. */
public final class GrpcRemoteAnalysisMetadataWriter implements RemoteAnalysisMetadataWriter {
  private final AnalysisCacheServiceGrpc.AnalysisCacheServiceBlockingStub blockingStub;
  private final Duration deadline;
  @Nullable private final GitRepositoryState gitRepositoryState;

  public GrpcRemoteAnalysisMetadataWriter(
      ManagedChannel channel,
      @Nullable CallCredentials callCredentials,
      Duration deadline,
      @Nullable GitRepositoryState gitRepositoryState) {
    AnalysisCacheServiceGrpc.AnalysisCacheServiceBlockingStub stub =
        AnalysisCacheServiceGrpc.newBlockingStub(channel);
    if (callCredentials != null) {
      stub = stub.withCallCredentials(callCredentials);
    }
    this.blockingStub = stub;
    this.deadline = deadline;
    this.gitRepositoryState = gitRepositoryState;
  }

  @Override
  public boolean addTopLevelTargets(
      String invocationId,
      long evaluatingVersion,
      String configurationHash,
      boolean useFakeStampData,
      String blazeVersion,
      Collection<String> targets,
      Collection<String> configFlags)
      throws IOException {
    BuildMetadata metadata =
        GrpcRemoteAnalysisCacheClient.buildMetadata(
            evaluatingVersion,
            configurationHash,
            useFakeStampData,
            blazeVersion,
            gitRepositoryState);
    WriteTopLevelTargetsRequest request =
        WriteTopLevelTargetsRequest.newBuilder()
            .setInvocationId(invocationId)
            .setBuild(metadata)
            .addAllTargets(targets)
            .addAllConfigFlags(configFlags)
            .build();
    try {
      WriteTopLevelTargetsResponse response =
          blockingStub
              .withDeadlineAfter(deadline.toMillis(), MILLISECONDS)
              .writeTopLevelTargets(request);
      if (!response.getOk() && !Strings.isNullOrEmpty(response.getMessage())) {
        throw new IOException(response.getMessage());
      }
      return response.getOk();
    } catch (StatusRuntimeException e) {
      throw new IOException(e);
    }
  }
}
