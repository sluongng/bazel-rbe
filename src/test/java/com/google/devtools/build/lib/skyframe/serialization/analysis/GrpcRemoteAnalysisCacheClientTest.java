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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.devtools.build.lib.events.StoredEventHandler;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisCacheBatchLookupRequest;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisCacheBatchLookupResponse;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisCacheKey;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisCacheLookupRequest;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisCacheLookupResponse;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisCacheServiceGrpc;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisValue;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.LookupResult;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.QueryTopLevelTargetsRequest;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.QueryTopLevelTargetsResponse;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.TopLevelTargetsMatchStatus;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class GrpcRemoteAnalysisCacheClientTest {
  private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();
  private final List<GrpcRemoteAnalysisCacheClient> clients = new ArrayList<>();
  private FakeAnalysisCacheService service;
  private Server server;
  private ManagedChannel channel;

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
  }

  @After
  public void tearDown() throws Exception {
    for (GrpcRemoteAnalysisCacheClient client : clients) {
      client.shutdown();
    }
    if (channel != null) {
      channel.shutdownNow();
    }
    if (server != null) {
      server.shutdownNow();
      server.awaitTermination();
    }
  }

  @Test
  public void lookup_returnsResponseAndUpdatesStats() throws Exception {
    AnalysisCacheKey key =
        AnalysisCacheKey.newBuilder()
            .setFingerprintHex("aabbccdd")
            .setSkyFunction("SKYFN")
            .setSkyKey("SKYKEY")
            .build();
    AnalysisCacheLookupResponse response =
        AnalysisCacheLookupResponse.newBuilder()
            .setKey(key)
            .setResult(LookupResult.LOOKUP_RESULT_HIT)
            .setValue(
                AnalysisValue.newBuilder()
                    .setSerializedValue(ByteString.copyFrom(new byte[] {1, 2, 3}))
                    .setSizeBytes(3)
                    .setValueDigestHex("010203")
                    .build())
            .build();
    service.setLookupResponse(
        AnalysisCacheBatchLookupResponse.newBuilder().addResponses(response).build());

    GrpcRemoteAnalysisCacheClient client = newClient(Duration.ZERO, /* metadataParams= */ null);

    AnalysisCacheLookupResponse actual =
        client.lookup(AnalysisCacheLookupRequest.newBuilder().setKey(key).build()).get(5, SECONDS);

    assertThat(actual).isEqualTo(response);
    RemoteAnalysisCacheClient.Stats stats = client.getStats();
    assertThat(stats.requestsSent()).isEqualTo(1);
    assertThat(stats.batches()).isEqualTo(1);
    assertThat(stats.bytesSent()).isEqualTo(4);
    assertThat(stats.bytesReceived()).isEqualTo(3);
  }

  @Test
  public void lookup_emptyResponse_returnsError() throws Exception {
    AnalysisCacheKey key = AnalysisCacheKey.newBuilder().setFingerprintHex("aabbccdd").build();
    service.setLookupResponse(AnalysisCacheBatchLookupResponse.getDefaultInstance());
    GrpcRemoteAnalysisCacheClient client = newClient(Duration.ZERO, /* metadataParams= */ null);

    AnalysisCacheLookupResponse actual =
        client.lookup(AnalysisCacheLookupRequest.newBuilder().setKey(key).build()).get(5, SECONDS);

    assertThat(actual.getResult()).isEqualTo(LookupResult.LOOKUP_RESULT_ERROR);
    assertThat(actual.getKey()).isEqualTo(key);
    assertThat(actual.getErrorMessage()).contains("empty response");
  }

  @Test
  public void lookup_retriesOnUnavailable() throws Exception {
    AnalysisCacheKey key = AnalysisCacheKey.newBuilder().setFingerprintHex("aabbccdd").build();
    AnalysisCacheLookupResponse response =
        AnalysisCacheLookupResponse.newBuilder()
            .setKey(key)
            .setResult(LookupResult.LOOKUP_RESULT_HIT)
            .build();
    service.setLookupResponse(
        AnalysisCacheBatchLookupResponse.newBuilder().addResponses(response).build());
    service.failNextLookups(1, Status.UNAVAILABLE);

    GrpcRemoteAnalysisCacheClient client =
        newClient(Duration.ofMillis(1), /* metadataParams= */ null);

    AnalysisCacheLookupResponse actual =
        client.lookup(AnalysisCacheLookupRequest.newBuilder().setKey(key).build()).get(5, SECONDS);

    assertThat(actual.getResult()).isEqualTo(LookupResult.LOOKUP_RESULT_HIT);
    assertThat(service.getLookupCallCount()).isEqualTo(2);
  }

  @Test
  public void lookupTopLevelTargets_sendsMetadataAndBailsOnNoMatch() throws Exception {
    service.setQueryResponse(
        QueryTopLevelTargetsResponse.newBuilder()
            .setMatchStatus(TopLevelTargetsMatchStatus.MATCH_STATUS_NO_MATCH)
            .build());

    SkycacheMetadataParamsImpl metadataParams = new SkycacheMetadataParamsImpl();
    metadataParams.init(
        /* clNumber= */ 0L,
        /* bazelVersion= */ "",
        /* targets= */ ImmutableList.of("//foo:bar"),
        /* useFakeStampData= */ false,
        /* userOptions= */ ImmutableMap.of("--compilation_mode=fastbuild", ""),
        /* projectSclOptions= */ ImmutableSet.of());
    metadataParams.setOriginalConfigurationOptions(ImmutableSet.of("compilation_mode"));

    GrpcRemoteAnalysisCacheClient client = newClient(Duration.ZERO, metadataParams);

    AtomicBoolean bailedOut = new AtomicBoolean(false);
    StoredEventHandler eventHandler = new StoredEventHandler();
    client.lookupTopLevelTargets(
        /* evaluatingVersion= */ 123L,
        /* configurationHash= */ "configHash",
        /* useFakeStampData= */ false,
        /* bazelVersion= */ "bazel-7.0.0",
        eventHandler,
        () -> bailedOut.set(true));

    QueryTopLevelTargetsRequest request = service.getLastQueryRequest();
    assertThat(request.getTargetsList()).containsExactly("//foo:bar");
    assertThat(request.getConfigFlagsList()).containsExactly("--compilation_mode=fastbuild");
    assertThat(request.getBuild().getEvaluatingVersion()).isEqualTo(123L);
    assertThat(request.getBuild().getConfigurationHash()).isEqualTo("configHash");
    assertThat(request.getBuild().getBazelVersion()).isEqualTo("bazel-7.0.0");
    assertThat(request.getBuild().getUseFakeStampData()).isFalse();
    assertThat(bailedOut.get()).isTrue();
    assertThat(client.getStats().matchStatus())
        .isEqualTo(TopLevelTargetsMatchStatus.MATCH_STATUS_NO_MATCH);
    assertThat(
            eventHandler.getEvents().stream().anyMatch(e -> e.getMessage().contains("no cached")))
        .isTrue();
  }

  private GrpcRemoteAnalysisCacheClient newClient(
      Duration retryInterval, SkycacheMetadataParamsImpl metadataParams) {
    RemoteAnalysisCachingOptions options = new RemoteAnalysisCachingOptions();
    options.deadline = Duration.ofSeconds(1);
    options.unreachableCacheRetryInterval = retryInterval;
    options.concurrency = 0;

    GrpcRemoteAnalysisCacheClient client =
        new GrpcRemoteAnalysisCacheClient(
            channel, /* callCredentials= */ null, options, null, metadataParams);
    clients.add(client);
    return client;
  }

  private static final class FakeAnalysisCacheService
      extends AnalysisCacheServiceGrpc.AnalysisCacheServiceImplBase {
    private final AtomicReference<AnalysisCacheBatchLookupRequest> lastLookupRequest =
        new AtomicReference<>();
    private final AtomicReference<QueryTopLevelTargetsRequest> lastQueryRequest =
        new AtomicReference<>();
    private final AtomicInteger lookupCallCount = new AtomicInteger();
    private final AtomicInteger failuresRemaining = new AtomicInteger();
    private volatile AnalysisCacheBatchLookupResponse lookupResponse =
        AnalysisCacheBatchLookupResponse.getDefaultInstance();
    private volatile QueryTopLevelTargetsResponse queryResponse =
        QueryTopLevelTargetsResponse.getDefaultInstance();
    private volatile Status failureStatus = Status.OK;

    void setLookupResponse(AnalysisCacheBatchLookupResponse response) {
      this.lookupResponse = response;
    }

    void setQueryResponse(QueryTopLevelTargetsResponse response) {
      this.queryResponse = response;
    }

    void failNextLookups(int count, Status status) {
      failuresRemaining.set(count);
      failureStatus = status;
    }

    int getLookupCallCount() {
      return lookupCallCount.get();
    }

    QueryTopLevelTargetsRequest getLastQueryRequest() {
      return lastQueryRequest.get();
    }

    @Override
    public void batchLookup(
        AnalysisCacheBatchLookupRequest request,
        StreamObserver<AnalysisCacheBatchLookupResponse> responseObserver) {
      lastLookupRequest.set(request);
      lookupCallCount.incrementAndGet();
      if (failuresRemaining.get() > 0) {
        failuresRemaining.decrementAndGet();
        responseObserver.onError(failureStatus.asRuntimeException());
        return;
      }
      responseObserver.onNext(lookupResponse);
      responseObserver.onCompleted();
    }

    @Override
    public void queryTopLevelTargets(
        QueryTopLevelTargetsRequest request,
        StreamObserver<QueryTopLevelTargetsResponse> responseObserver) {
      lastQueryRequest.set(request);
      responseObserver.onNext(queryResponse);
      responseObserver.onCompleted();
    }
  }
}
