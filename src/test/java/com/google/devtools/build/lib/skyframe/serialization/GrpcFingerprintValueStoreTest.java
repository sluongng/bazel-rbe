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

import static com.google.common.truth.Truth.assertThat;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertThrows;

import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.devtools.build.lib.skyframe.serialization.FingerprintValueStore.MissingFingerprintValueException;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisCacheServiceGrpc;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.FingerprintKey;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.GetValuesRequest;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.GetValuesResponse;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.KeyValue;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.PutValueResult;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.PutValuesRequest;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.PutValuesResponse;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class GrpcFingerprintValueStoreTest {
  private static final BaseEncoding HEX = BaseEncoding.base16().lowerCase();
  private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();
  private FakeAnalysisCacheService service;
  private Server server;
  private ManagedChannel channel;
  private GrpcFingerprintValueStore store;

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
    store =
        new GrpcFingerprintValueStore(channel, /* callCredentials= */ null, Duration.ofSeconds(1));
  }

  @After
  public void tearDown() throws Exception {
    if (channel != null) {
      channel.shutdownNow();
    }
    if (server != null) {
      server.shutdownNow();
      server.awaitTermination();
    }
  }

  @Test
  public void putThenGet_roundTripsThroughGrpc() throws Exception {
    StringKey key = new StringKey("abc");
    byte[] value = new byte[] {1, 2, 3};

    WriteStatuses.WriteStatus status = store.put(key, value);
    status.get(5, SECONDS);

    PutValuesRequest putRequest = service.getLastPutRequest();
    assertThat(putRequest.getValuesCount()).isEqualTo(1);
    KeyValue stored = putRequest.getValues(0);
    assertThat(stored.getKey().getFingerprintHex()).isEqualTo(HEX.encode(key.toBytes()));
    assertThat(stored.getValue().toByteArray()).isEqualTo(value);
    assertThat(stored.getValueSize()).isEqualTo(value.length);
    assertThat(stored.getValueDigestHex()).isEqualTo(Hashing.sha256().hashBytes(value).toString());

    byte[] fetched = store.get(key).get(5, SECONDS);
    assertThat(fetched).isEqualTo(value);
    assertThat(service.getGetCallCount()).isEqualTo(1);
    assertThat(service.getPutCallCount()).isEqualTo(1);
  }

  @Test
  public void get_missingValueFails() throws Exception {
    StringKey key = new StringKey("missing");
    ExecutionException error =
        assertThrows(ExecutionException.class, () -> store.get(key).get(5, SECONDS));
    assertThat(error.getCause()).isInstanceOf(MissingFingerprintValueException.class);
  }

  private static final class FakeAnalysisCacheService
      extends AnalysisCacheServiceGrpc.AnalysisCacheServiceImplBase {
    private final Map<String, byte[]> values = new ConcurrentHashMap<>();
    private final AtomicReference<PutValuesRequest> lastPutRequest = new AtomicReference<>();
    private final AtomicReference<GetValuesRequest> lastGetRequest = new AtomicReference<>();
    private final AtomicInteger putCallCount = new AtomicInteger();
    private final AtomicInteger getCallCount = new AtomicInteger();

    PutValuesRequest getLastPutRequest() {
      return lastPutRequest.get();
    }

    int getPutCallCount() {
      return putCallCount.get();
    }

    int getGetCallCount() {
      return getCallCount.get();
    }

    @Override
    public void putValues(
        PutValuesRequest request, StreamObserver<PutValuesResponse> responseObserver) {
      lastPutRequest.set(request);
      putCallCount.incrementAndGet();
      PutValuesResponse.Builder response = PutValuesResponse.newBuilder();
      for (KeyValue value : request.getValuesList()) {
        values.put(value.getKey().getFingerprintHex(), value.getValue().toByteArray());
        response.addResults(PutValueResult.newBuilder().setKey(value.getKey()).setOk(true).build());
      }
      responseObserver.onNext(response.build());
      responseObserver.onCompleted();
    }

    @Override
    public void getValues(
        GetValuesRequest request, StreamObserver<GetValuesResponse> responseObserver) {
      lastGetRequest.set(request);
      getCallCount.incrementAndGet();
      GetValuesResponse.Builder response = GetValuesResponse.newBuilder();
      for (FingerprintKey key : request.getKeysList()) {
        byte[] value = values.get(key.getFingerprintHex());
        if (value == null) {
          response.addMissing(key);
        } else {
          response.addValues(
              KeyValue.newBuilder()
                  .setKey(key)
                  .setValue(ByteString.copyFrom(value))
                  .setValueSize(value.length)
                  .setValueDigestHex(Hashing.sha256().hashBytes(value).toString()));
        }
      }
      responseObserver.onNext(response.build());
      responseObserver.onCompleted();
    }
  }
}
