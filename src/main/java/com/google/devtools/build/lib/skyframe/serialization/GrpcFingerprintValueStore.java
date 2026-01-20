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

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.common.base.Strings;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.devtools.build.lib.skyframe.serialization.FingerprintValueStore.MissingFingerprintValueException;
import com.google.devtools.build.lib.skyframe.serialization.WriteStatuses.SettableWriteStatus;
import com.google.devtools.build.lib.skyframe.serialization.WriteStatuses.WriteStatus;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisCacheServiceGrpc;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.FingerprintKey;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.GetValuesRequest;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.GetValuesResponse;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.KeyValue;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.PutValuesRequest;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.PutValuesResponse;
import com.google.protobuf.ByteString;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;

/** A gRPC-backed {@link FingerprintValueStore}. */
public final class GrpcFingerprintValueStore implements FingerprintValueStore {
  private static final BaseEncoding HEX = BaseEncoding.base16().lowerCase();

  private final AnalysisCacheServiceGrpc.AnalysisCacheServiceFutureStub futureStub;
  private final Duration deadline;

  private final AtomicLong valueBytesReceived = new AtomicLong();
  private final AtomicLong valueBytesSent = new AtomicLong();
  private final AtomicLong keyBytesSent = new AtomicLong();
  private final AtomicLong entriesWritten = new AtomicLong();
  private final AtomicLong entriesFound = new AtomicLong();
  private final AtomicLong entriesNotFound = new AtomicLong();
  private final AtomicLong getBatches = new AtomicLong();
  private final AtomicLong setBatches = new AtomicLong();

  public GrpcFingerprintValueStore(
      ManagedChannel channel, @Nullable CallCredentials callCredentials, Duration deadline) {
    AnalysisCacheServiceGrpc.AnalysisCacheServiceFutureStub stub =
        AnalysisCacheServiceGrpc.newFutureStub(channel);
    if (callCredentials != null) {
      stub = stub.withCallCredentials(callCredentials);
    }
    this.futureStub = stub;
    this.deadline = deadline;
  }

  @Override
  public WriteStatus put(KeyBytesProvider fingerprint, byte[] serializedBytes) {
    String keyHex = toHex(fingerprint.toBytes());
    ByteString valueBytes = ByteString.copyFrom(serializedBytes);
    KeyValue keyValue =
        KeyValue.newBuilder()
            .setKey(FingerprintKey.newBuilder().setFingerprintHex(keyHex))
            .setValue(valueBytes)
            .setValueSize(serializedBytes.length)
            .setValueDigestHex(hashBytes(serializedBytes))
            .build();
    PutValuesRequest request = PutValuesRequest.newBuilder().addValues(keyValue).build();

    keyBytesSent.addAndGet(fingerprint.toBytes().length);
    valueBytesSent.addAndGet(serializedBytes.length);
    setBatches.incrementAndGet();

    ListenableFuture<PutValuesResponse> responseFuture =
        futureStub.withDeadlineAfter(deadline.toMillis(), MILLISECONDS).putValues(request);

    SettableWriteStatus status = new SettableWriteStatus();
    Futures.addCallback(
        responseFuture,
        new FutureCallback<>() {
          @Override
          public void onSuccess(PutValuesResponse response) {
            boolean ok =
                response.getResultsCount() == 0
                    || response.getResultsList().stream().allMatch(r -> r.getOk());
            if (ok) {
              entriesWritten.incrementAndGet();
              status.markSuccess();
            } else {
              status.failWith(
                  new IOException(
                      response.getResultsList().stream()
                          .map(r -> r.getErrorMessage())
                          .filter(message -> !Strings.isNullOrEmpty(message))
                          .findFirst()
                          .orElse("Remote value store write failed")));
            }
          }

          @Override
          public void onFailure(Throwable t) {
            status.failWith(t);
          }
        },
        directExecutor());
    return status;
  }

  @Override
  public ListenableFuture<byte[]> get(KeyBytesProvider fingerprint) throws IOException {
    String keyHex = toHex(fingerprint.toBytes());
    GetValuesRequest request =
        GetValuesRequest.newBuilder()
            .addKeys(FingerprintKey.newBuilder().setFingerprintHex(keyHex))
            .build();

    keyBytesSent.addAndGet(fingerprint.toBytes().length);
    getBatches.incrementAndGet();

    ListenableFuture<GetValuesResponse> responseFuture =
        futureStub.withDeadlineAfter(deadline.toMillis(), MILLISECONDS).getValues(request);

    return Futures.transformAsync(
        responseFuture,
        response -> {
          if (response.getMissingCount() > 0 || response.getValuesCount() == 0) {
            entriesNotFound.incrementAndGet();
            return Futures.immediateFailedFuture(new MissingFingerprintValueException(fingerprint));
          }
          KeyValue value = response.getValues(0);
          valueBytesReceived.addAndGet(value.getValue().size());
          entriesFound.incrementAndGet();
          return Futures.immediateFuture(value.getValue().toByteArray());
        },
        directExecutor());
  }

  @Override
  public Stats getStats() {
    return new Stats(
        valueBytesReceived.get(),
        valueBytesSent.get(),
        keyBytesSent.get(),
        entriesWritten.get(),
        entriesFound.get(),
        entriesNotFound.get(),
        getBatches.get(),
        setBatches.get());
  }

  private static String toHex(byte[] bytes) {
    return HEX.encode(bytes);
  }

  private static String hashBytes(byte[] bytes) {
    return Hashing.sha256().hashBytes(bytes).toString();
  }
}
