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

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.concurrent.ForkJoinPool.commonPool;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.devtools.build.lib.authandtls.AuthAndTLSOptions;
import com.google.devtools.build.lib.authandtls.GoogleAuthUtils;
import com.google.devtools.build.lib.skyframe.serialization.FingerprintValueCache;
import com.google.devtools.build.lib.skyframe.serialization.FingerprintValueService;
import com.google.devtools.build.lib.skyframe.serialization.FingerprintValueStore;
import com.google.devtools.build.lib.skyframe.serialization.GrpcFingerprintValueStore;
import com.google.devtools.build.lib.skyframe.serialization.SkycacheMetadataParams;
import com.google.devtools.build.lib.util.AbruptExitException;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import java.io.IOException;
import javax.annotation.Nullable;

/** Default remote analysis caching services supplier backed by gRPC. */
public final class GrpcRemoteAnalysisCachingServicesSupplier
    implements RemoteAnalysisCachingServicesSupplier {
  @Nullable private FingerprintValueService fingerprintValueService;
  @Nullable private RemoteAnalysisCacheClient analysisCacheClient;
  @Nullable private RemoteAnalysisMetadataWriter metadataWriter;
  @Nullable private SkycacheMetadataParams metadataParams;

  @Nullable private ManagedChannel storeChannel;
  @Nullable private ManagedChannel analysisChannel;

  @Nullable private GitRepositoryState gitRepositoryState;
  @Nullable private CallCredentials callCredentials;

  @Override
  public synchronized void configure(
      RemoteAnalysisCachingOptions cachingOptions,
      @Nullable ClientId clientId,
      String buildId,
      @Nullable RemoteAnalysisJsonLogWriter jsonLogWriter,
      @Nullable AuthAndTLSOptions authAndTlsOptions,
      @Nullable GitRepositoryState gitRepositoryState)
      throws AbruptExitException {
    this.gitRepositoryState = gitRepositoryState;
    if (this.metadataParams == null) {
      this.metadataParams = new SkycacheMetadataParamsImpl();
    }
    this.callCredentials = null;
    if (authAndTlsOptions != null) {
      try {
        this.callCredentials = GoogleAuthUtils.newGoogleCallCredentials(authAndTlsOptions);
      } catch (IOException e) {
        // Continue without credentials; failures are handled during RPCs.
        this.callCredentials = null;
      }
    }

    String storeTarget = pickStoreTarget(cachingOptions);
    String analysisTarget =
        Strings.isNullOrEmpty(cachingOptions.analysisCacheService)
            ? null
            : cachingOptions.analysisCacheService;

    closeChannels();

    this.storeChannel = createChannel(storeTarget, authAndTlsOptions);
    this.analysisChannel =
        analysisTarget == null ? null : createChannel(analysisTarget, authAndTlsOptions);

    FingerprintValueStore store =
        storeChannel == null
            ? FingerprintValueStore.inMemoryStore()
            : new GrpcFingerprintValueStore(storeChannel, callCredentials, cachingOptions.deadline);

    this.fingerprintValueService =
        new FingerprintValueService(
            commonPool(),
            store,
            new FingerprintValueCache(FingerprintValueCache.SyncMode.NOT_LINKED),
            FingerprintValueService.NONPROD_FINGERPRINTER,
            jsonLogWriter);

    if (analysisChannel != null) {
      this.analysisCacheClient =
          new GrpcRemoteAnalysisCacheClient(
              analysisChannel, callCredentials, cachingOptions, gitRepositoryState, metadataParams);
      this.metadataWriter =
          new GrpcRemoteAnalysisMetadataWriter(
              analysisChannel, callCredentials, SkycacheMetadataParams.TIMEOUT, gitRepositoryState);
    } else {
      this.analysisCacheClient = null;
      this.metadataWriter = null;
    }
  }

  @Override
  public synchronized ListenableFuture<FingerprintValueService> getFingerprintValueService() {
    return immediateFuture(fingerprintValueService);
  }

  @Override
  public synchronized ListenableFuture<? extends RemoteAnalysisCacheClient>
      getAnalysisCacheClient() {
    return analysisCacheClient == null ? null : immediateFuture(analysisCacheClient);
  }

  @Override
  public synchronized ListenableFuture<? extends RemoteAnalysisMetadataWriter> getMetadataWriter() {
    return metadataWriter == null ? null : immediateFuture(metadataWriter);
  }

  @Override
  public synchronized SkycacheMetadataParams getSkycacheMetadataParams() {
    return metadataParams;
  }

  @Override
  public synchronized void shutdown() {
    if (fingerprintValueService != null) {
      fingerprintValueService.shutdown();
      fingerprintValueService = null;
    }
    if (analysisCacheClient instanceof GrpcRemoteAnalysisCacheClient grpcClient) {
      grpcClient.shutdown();
    }
    closeChannels();
  }

  private void closeChannels() {
    if (storeChannel != null) {
      storeChannel.shutdown();
      storeChannel = null;
    }
    if (analysisChannel != null) {
      analysisChannel.shutdown();
      analysisChannel = null;
    }
  }

  @Nullable
  private static ManagedChannel createChannel(
      @Nullable String target, @Nullable AuthAndTLSOptions authAndTlsOptions)
      throws AbruptExitException {
    if (Strings.isNullOrEmpty(target)) {
      return null;
    }
    AuthAndTLSOptions options =
        authAndTlsOptions == null ? new AuthAndTLSOptions() : authAndTlsOptions;
    try {
      return GoogleAuthUtils.newChannel(
          /* executor= */ null, target, /* proxy= */ "", options, /* interceptors= */ null);
    } catch (IOException e) {
      return null;
    }
  }

  @Nullable
  private static String pickStoreTarget(RemoteAnalysisCachingOptions options) {
    if (!Strings.isNullOrEmpty(options.remoteAnalysisWriteProxy)) {
      return options.remoteAnalysisWriteProxy;
    }
    if (!Strings.isNullOrEmpty(options.remoteAnalysisCache)) {
      return options.remoteAnalysisCache;
    }
    if (!Strings.isNullOrEmpty(options.analysisCacheService)) {
      return options.analysisCacheService;
    }
    return null;
  }
}
