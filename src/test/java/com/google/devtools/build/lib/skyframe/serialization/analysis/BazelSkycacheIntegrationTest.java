// Copyright 2024 The Bazel Authors. All rights reserved.
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
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.devtools.build.lib.skyframe.serialization.analysis.LongVersionGetterTestInjection.injectVersionGetterForTesting;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.devtools.build.lib.events.EventHandler;
import com.google.devtools.build.lib.runtime.BlazeRuntime;
import com.google.devtools.build.lib.skyframe.SkyFunctions;
import com.google.devtools.build.lib.skyframe.serialization.FingerprintValueCache;
import com.google.devtools.build.lib.skyframe.serialization.FingerprintValueService;
import com.google.devtools.build.lib.skyframe.serialization.FingerprintValueStore;
import com.google.devtools.build.lib.skyframe.serialization.KeyBytesProvider;
import com.google.devtools.build.lib.skyframe.serialization.PackedFingerprint;
import com.google.devtools.build.lib.skyframe.serialization.SerializationModule;
import com.google.devtools.build.lib.skyframe.serialization.SkycacheMetadataParams;
import com.google.devtools.build.lib.skyframe.serialization.WriteStatuses;
import com.google.devtools.build.lib.skyframe.serialization.WriteStatuses.WriteStatus;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisCacheLookupRequest;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisCacheLookupResponse;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisValue;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.LookupResult;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.TopLevelTargetsMatchStatus;
import com.google.devtools.build.lib.skyframe.serialization.proto.DataType;
import com.google.devtools.build.lib.util.AbruptExitException;
import com.google.devtools.build.lib.versioning.LongVersionGetter;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class BazelSkycacheIntegrationTest extends SkycacheIntegrationTestBase {
  private final LongVersionGetter versionGetter = mock(LongVersionGetter.class);
  private static final FailingFingerprintValueStore failingStore =
      new FailingFingerprintValueStore();
  private static final InMemoryAnalysisCacheClient analysisCacheClient =
      new InMemoryAnalysisCacheClient(failingStore);
  private static final NoopMetadataWriter metadataWriter = new NoopMetadataWriter();

  @Before
  public void injectVersionGetter() {
    injectVersionGetterForTesting(versionGetter);
  }

  @Before
  public void resetFailingStore() {
    failingStore.reset();
    failingStore.clearValues();
    analysisCacheClient.resetStats();
  }

  private static class FailingFingerprintValueStore implements FingerprintValueStore {
    private final FingerprintValueStore delegate = FingerprintValueStore.inMemoryStore();
    private final AtomicBoolean shouldFail = new AtomicBoolean();
    private final AtomicInteger failCounter = new AtomicInteger();
    private final AtomicReference<KeyBytesProvider> lastFailedKey = new AtomicReference<>();
    private final ConcurrentHashMap<String, byte[]> valuesByHex = new ConcurrentHashMap<>();

    private void failNextPut() {
      shouldFail.set(true);
    }

    private int getFailCounter() {
      return failCounter.get();
    }

    private KeyBytesProvider getFailedKey() {
      return lastFailedKey.get();
    }

    private void reset() {
      shouldFail.set(false);
      failCounter.set(0);
      lastFailedKey.set(null);
    }

    private void clearValues() {
      valuesByHex.clear();
    }

    private byte[] getValueByHex(String hex) {
      return valuesByHex.get(hex);
    }

    @Override
    public WriteStatus put(KeyBytesProvider fingerprint, byte[] serializedBytes) {
      if (shouldFail.getAndSet(false)) {
        failCounter.getAndIncrement();
        lastFailedKey.set(fingerprint);
        return WriteStatuses.immediateFailedWriteStatus(
            new IOException("Simulated write failure for " + fingerprint));
      }
      valuesByHex.put(bytesToHex(fingerprint.toBytes()), stripInvalidationHeader(serializedBytes));
      return delegate.put(fingerprint, serializedBytes);
    }

    @Override
    public ListenableFuture<byte[]> get(KeyBytesProvider fingerprint) throws IOException {
      return delegate.get(fingerprint);
    }
  }

  private static final class InMemoryAnalysisCacheClient implements RemoteAnalysisCacheClient {
    private final FailingFingerprintValueStore store;
    private final AtomicLong bytesSent = new AtomicLong();
    private final AtomicLong bytesReceived = new AtomicLong();
    private final AtomicLong requestsSent = new AtomicLong();
    private final AtomicLong batches = new AtomicLong();
    private final AtomicReference<TopLevelTargetsMatchStatus> matchStatus =
        new AtomicReference<>(TopLevelTargetsMatchStatus.MATCH_STATUS_UNSPECIFIED);

    private InMemoryAnalysisCacheClient(FailingFingerprintValueStore store) {
      this.store = store;
    }

    void resetStats() {
      bytesSent.set(0);
      bytesReceived.set(0);
      requestsSent.set(0);
      batches.set(0);
      matchStatus.set(TopLevelTargetsMatchStatus.MATCH_STATUS_UNSPECIFIED);
    }

    @Override
    public ListenableFuture<AnalysisCacheLookupResponse> lookup(
        AnalysisCacheLookupRequest request) {
      requestsSent.incrementAndGet();
      batches.incrementAndGet();
      bytesSent.addAndGet(estimateKeyBytes(request));
      AnalysisCacheLookupResponse response;
      if (!request.hasKey()) {
        response =
            AnalysisCacheLookupResponse.newBuilder()
                .setResult(LookupResult.LOOKUP_RESULT_ERROR)
                .setErrorMessage("missing key")
                .build();
      } else {
        String hex = request.getKey().getFingerprintHex();
        byte[] value = store.getValueByHex(hex);
        if (value == null) {
          response =
              AnalysisCacheLookupResponse.newBuilder()
                  .setKey(request.getKey())
                  .setResult(LookupResult.LOOKUP_RESULT_MISS)
                  .build();
        } else {
          bytesReceived.addAndGet(value.length);
          response =
              AnalysisCacheLookupResponse.newBuilder()
                  .setKey(request.getKey())
                  .setResult(LookupResult.LOOKUP_RESULT_HIT)
                  .setValue(
                      AnalysisValue.newBuilder()
                          .setSerializedValue(ByteString.copyFrom(value))
                          .setSizeBytes(value.length)
                          .setValueDigestHex(hashBytes(value)))
                  .build();
        }
      }
      return immediateFuture(response);
    }

    @Override
    public Stats getStats() {
      return new Stats(
          bytesSent.get(),
          bytesReceived.get(),
          requestsSent.get(),
          batches.get(),
          matchStatus.get());
    }

    @Override
    public void lookupTopLevelTargets(
        long evaluatingVersion,
        String configurationHash,
        boolean useFakeStampData,
        String bazelVersion,
        EventHandler eventHandler,
        Runnable bailOutCallback) {
      matchStatus.set(TopLevelTargetsMatchStatus.MATCH_STATUS_MATCH);
    }
  }

  private static final class NoopMetadataWriter implements RemoteAnalysisMetadataWriter {
    @Override
    public boolean addTopLevelTargets(
        String invocationId,
        long evaluatingVersion,
        String configurationHash,
        boolean useFakeStampData,
        String blazeVersion,
        java.util.Collection<String> targets,
        java.util.Collection<String> configFlags) {
      return true;
    }
  }

  private static class ModuleWithOverrides extends SerializationModule {
    @Override
    protected RemoteAnalysisCachingServicesSupplier getAnalysisCachingServicesSupplier() {
      return new TestServicesSupplier(failingStore);
    }
  }

  private static class TestServicesSupplier implements RemoteAnalysisCachingServicesSupplier {
    private final ListenableFuture<FingerprintValueService> wrappedService;
    private final ListenableFuture<? extends RemoteAnalysisCacheClient> wrappedClient;
    private final ListenableFuture<? extends RemoteAnalysisMetadataWriter> wrappedWriter;
    private SkycacheMetadataParams metadataParams;

    private TestServicesSupplier(FailingFingerprintValueStore failingStore) {
      this.wrappedService =
          immediateFuture(
              new FingerprintValueService(
                  newSingleThreadExecutor(),
                  failingStore,
                  new FingerprintValueCache(FingerprintValueCache.SyncMode.NOT_LINKED),
                  FingerprintValueService.NONPROD_FINGERPRINTER,
                  /* jsonLogWriter= */ null));
      this.wrappedClient = immediateFuture(analysisCacheClient);
      this.wrappedWriter = immediateFuture(metadataWriter);
      this.metadataParams = new SkycacheMetadataParamsImpl();
    }

    @Override
    public ListenableFuture<FingerprintValueService> getFingerprintValueService() {
      return wrappedService;
    }

    @Override
    public ListenableFuture<? extends RemoteAnalysisCacheClient> getAnalysisCacheClient() {
      return wrappedClient;
    }

    @Override
    public ListenableFuture<? extends RemoteAnalysisMetadataWriter> getMetadataWriter() {
      return wrappedWriter;
    }

    @Override
    public SkycacheMetadataParams getSkycacheMetadataParams() {
      return metadataParams;
    }

    @Override
    public void shutdown() {}
  }

  @Override
  protected BlazeRuntime.Builder getRuntimeBuilder() throws Exception {
    return super.getRuntimeBuilder().addBlazeModule(new ModuleWithOverrides());
  }

  @Test
  public void buildCommand_uploadsFrontierBytesWithUploadMode() throws Exception {
    setupScenarioWithAspects();
    assertUploadSuccess("//bar:one");

    var listener = getCommandEnvironment().getRemoteAnalysisCachingEventListener();
    assertThat(listener.getSerializedKeysCount()).isAtLeast(9); // for Bazel
    assertThat(listener.getSkyfunctionCounts().count(SkyFunctions.CONFIGURED_TARGET))
        .isAtLeast(9); // for Bazel
  }

  @Test
  public void buildCommand_withWriteFailure_reportsErrorAndCompletes() throws Exception {
    setupScenarioWithAspects();

    failingStore.failNextPut();

    addOptions(UPLOAD_MODE_OPTION);
    var thrown = assertThrows(AbruptExitException.class, () -> buildTarget("//bar:one"));
    assertThat(thrown)
        .hasMessageThat()
        .contains(
            "java.io.IOException: Simulated write failure for " + failingStore.getFailedKey());

    assertThat(failingStore.getFailCounter()).isEqualTo(1);
    assertContainsEvent(
        "java.io.IOException: Simulated write failure for " + failingStore.getFailedKey());
  }

  @Test
  public void downloadAfterUpload_usesRemoteAnalysisCache() throws Exception {
    setupScenarioWithConfiguredTargets();

    addOptions(UPLOAD_MODE_OPTION);
    buildTarget("//foo:A");

    getSkyframeExecutor().resetEvaluator();

    addOptions(DOWNLOAD_MODE_OPTION);
    buildTarget("//foo:A");

    assertThat(getCommandEnvironment().getRemoteAnalysisCachingEventListener().getCacheHits())
        .isNotEmpty();
  }

  @Test
  public void downloadWithDifferentDistinguisher_hasNoCacheHits() throws Exception {
    setupScenarioWithConfiguredTargets();

    // Simulates a different workspace version (for example, a newer git commit).
    addOptions(
        UPLOAD_MODE_OPTION, "--experimental_analysis_cache_key_distinguisher_for_testing=base");
    buildTarget("//foo:A");

    getSkyframeExecutor().resetEvaluator();

    addOptions(
        DOWNLOAD_MODE_OPTION, "--experimental_analysis_cache_key_distinguisher_for_testing=next");
    buildTarget("//foo:A");

    assertThat(getCommandEnvironment().getRemoteAnalysisCachingEventListener().getCacheHits())
        .isEmpty();
  }

  private static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  private static String hashBytes(byte[] bytes) {
    return Hashing.sha256().hashBytes(bytes).toString();
  }

  private static long estimateKeyBytes(AnalysisCacheLookupRequest request) {
    if (!request.hasKey()) {
      return 0;
    }
    String hex = request.getKey().getFingerprintHex();
    if (hex.isEmpty()) {
      return 0;
    }
    return (hex.length() + 1L) / 2L;
  }

  private static byte[] stripInvalidationHeader(byte[] bytes) {
    CodedInputStream codedIn = CodedInputStream.newInstance(bytes);
    try {
      int dataTypeOrdinal = codedIn.readInt32();
      DataType dataType = DataType.forNumber(dataTypeOrdinal);
      if (dataType == null) {
        return bytes;
      }
      switch (dataType) {
        case DATA_TYPE_EMPTY -> {}
        case DATA_TYPE_FILE, DATA_TYPE_LISTING -> codedIn.readString();
        case DATA_TYPE_ANALYSIS_NODE, DATA_TYPE_EXECUTION_NODE ->
            PackedFingerprint.readFrom(codedIn);
        default -> {
          return bytes;
        }
      }
      int offset = codedIn.getTotalBytesRead();
      if (offset <= 0 || offset >= bytes.length) {
        return bytes;
      }
      return Arrays.copyOfRange(bytes, offset, bytes.length);
    } catch (IOException e) {
      return bytes;
    }
  }
}
