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

import static com.google.common.truth.Truth.assertThat;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.collect.ImmutableList;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisCacheServiceGrpc;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.WriteTopLevelTargetsRequest;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.WriteTopLevelTargetsResponse;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class GrpcRemoteAnalysisMetadataWriterTest {
  private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();
  private FakeAnalysisCacheService service;
  private Server server;
  private ManagedChannel channel;
  private GrpcRemoteAnalysisMetadataWriter writer;

  @Before
  public void setUp() throws Exception {
    service = new FakeAnalysisCacheService();
    serviceRegistry.addService(service);

    String serverName = InProcessServerBuilder.generateName();
    server =
        InProcessServerBuilder.forName(serverName)
            .fallbackHandlerRegistry(serviceRegistry)
            .directExecutor()
            .build()
            .start();
    channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
    writer =
        new GrpcRemoteAnalysisMetadataWriter(
            channel,
            /* callCredentials= */ null,
            Duration.ofSeconds(1),
            /* gitRepositoryState= */ null);
  }

  @After
  public void tearDown() throws Exception {
    if (channel != null) {
      channel.shutdownNow();
    }
    if (server != null) {
      server.shutdownNow();
      server.awaitTermination(5, SECONDS);
    }
  }

  @Test
  public void addTopLevelTargets_sendsExpectedRequest() throws Exception {
    service.setResponse(WriteTopLevelTargetsResponse.newBuilder().setOk(true).build());

    boolean ok =
        writer.addTopLevelTargets(
            "invocation-123",
            /* evaluatingVersion= */ 42L,
            "config-hash",
            /* useFakeStampData= */ false,
            "bazel-7.0.0",
            ImmutableList.of("//foo:bar"),
            ImmutableList.of("--compilation_mode=fastbuild"));

    assertThat(ok).isTrue();
    WriteTopLevelTargetsRequest request = service.getLastWriteRequest();
    assertThat(request.getInvocationId()).isEqualTo("invocation-123");
    assertThat(request.getTargetsList()).containsExactly("//foo:bar");
    assertThat(request.getConfigFlagsList()).containsExactly("--compilation_mode=fastbuild");
    assertThat(request.getBuild().getEvaluatingVersion()).isEqualTo(42L);
    assertThat(request.getBuild().getConfigurationHash()).isEqualTo("config-hash");
    assertThat(request.getBuild().getBazelVersion()).isEqualTo("bazel-7.0.0");
    assertThat(request.getBuild().getUseFakeStampData()).isFalse();
  }

  private static final class FakeAnalysisCacheService
      extends AnalysisCacheServiceGrpc.AnalysisCacheServiceImplBase {
    private final AtomicReference<WriteTopLevelTargetsRequest> lastWriteRequest =
        new AtomicReference<>();
    private volatile WriteTopLevelTargetsResponse response =
        WriteTopLevelTargetsResponse.getDefaultInstance();

    void setResponse(WriteTopLevelTargetsResponse response) {
      this.response = response;
    }

    WriteTopLevelTargetsRequest getLastWriteRequest() {
      return lastWriteRequest.get();
    }

    @Override
    public void writeTopLevelTargets(
        WriteTopLevelTargetsRequest request,
        StreamObserver<WriteTopLevelTargetsResponse> responseObserver) {
      lastWriteRequest.set(request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }
  }
}
