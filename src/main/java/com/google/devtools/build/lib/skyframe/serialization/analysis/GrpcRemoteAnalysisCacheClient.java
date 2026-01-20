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

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.devtools.build.lib.events.Event;
import com.google.devtools.build.lib.events.EventHandler;
import com.google.devtools.build.lib.skyframe.serialization.SkycacheMetadataParams;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisCacheBatchLookupRequest;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisCacheBatchLookupResponse;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisCacheLookupRequest;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisCacheLookupResponse;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisCacheServiceGrpc;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.BuildMetadata;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.GitCommitCandidate;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.GitWorkspaceVersion;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.LookupResult;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.QueryTopLevelTargetsRequest;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.QueryTopLevelTargetsResponse;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.TopLevelTargetsMatchStatus;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.WorkspaceVersion;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/** gRPC implementation of {@link RemoteAnalysisCacheClient}. */
public final class GrpcRemoteAnalysisCacheClient implements RemoteAnalysisCacheClient {
  private final AnalysisCacheServiceGrpc.AnalysisCacheServiceFutureStub futureStub;
  private final AnalysisCacheServiceGrpc.AnalysisCacheServiceBlockingStub blockingStub;
  private final Duration deadline;
  private final Duration unreachableRetryInterval;
  @Nullable private final Semaphore concurrencyLimiter;
  private final ScheduledExecutorService retryScheduler;
  @Nullable private final AtomicReference<GitRepositoryState> gitRepositoryStateRef;
  @Nullable private final SkycacheMetadataParams metadataParams;

  private final AtomicLong bytesSent = new AtomicLong();
  private final AtomicLong bytesReceived = new AtomicLong();
  private final AtomicLong requestsSent = new AtomicLong();
  private final AtomicLong batches = new AtomicLong();
  private final AtomicReference<TopLevelTargetsMatchStatus> matchStatus =
      new AtomicReference<>(TopLevelTargetsMatchStatus.MATCH_STATUS_UNSPECIFIED);
  @Nullable private volatile String selectedCommit;

  public GrpcRemoteAnalysisCacheClient(
      ManagedChannel channel,
      @Nullable CallCredentials callCredentials,
      RemoteAnalysisCachingOptions options,
      @Nullable GitRepositoryState gitRepositoryState,
      @Nullable SkycacheMetadataParams metadataParams) {
    AnalysisCacheServiceGrpc.AnalysisCacheServiceFutureStub future =
        AnalysisCacheServiceGrpc.newFutureStub(channel);
    AnalysisCacheServiceGrpc.AnalysisCacheServiceBlockingStub blocking =
        AnalysisCacheServiceGrpc.newBlockingStub(channel);
    if (callCredentials != null) {
      future = future.withCallCredentials(callCredentials);
      blocking = blocking.withCallCredentials(callCredentials);
    }
    this.futureStub = future;
    this.blockingStub = blocking;
    this.deadline = options.deadline;
    this.unreachableRetryInterval = options.unreachableCacheRetryInterval;
    this.concurrencyLimiter = options.concurrency > 0 ? new Semaphore(options.concurrency) : null;
    this.retryScheduler = Executors.newSingleThreadScheduledExecutor();
    this.gitRepositoryStateRef =
        gitRepositoryState == null ? null : new AtomicReference<>(gitRepositoryState);
    this.metadataParams = metadataParams;
  }

  @Override
  public ListenableFuture<AnalysisCacheLookupResponse> lookup(AnalysisCacheLookupRequest request) {
    AnalysisCacheLookupRequest enriched = maybeAugmentRequest(request);
    requestsSent.incrementAndGet();
    bytesSent.addAndGet(estimateKeyBytes(enriched));

    AnalysisCacheBatchLookupRequest batchRequest =
        AnalysisCacheBatchLookupRequest.newBuilder().addRequests(enriched).build();

    ListenableFuture<AnalysisCacheBatchLookupResponse> responseFuture =
        submitBatchLookup(batchRequest);

    return Futures.transform(
        responseFuture,
        response -> {
          if (response.getResponsesCount() == 0) {
            return AnalysisCacheLookupResponse.newBuilder()
                .setKey(enriched.getKey())
                .setResult(LookupResult.LOOKUP_RESULT_ERROR)
                .setErrorMessage("empty response from analysis cache service")
                .build();
          }
          AnalysisCacheLookupResponse first = response.getResponses(0);
          if (first.getResult() == LookupResult.LOOKUP_RESULT_HIT) {
            bytesReceived.addAndGet(first.getValue().getSerializedValue().size());
          }
          return first;
        },
        directExecutor());
  }

  @Override
  public Stats getStats() {
    return new Stats(
        bytesSent.get(), bytesReceived.get(), requestsSent.get(), batches.get(), matchStatus.get());
  }

  @Nullable
  public String getSelectedCommit() {
    return selectedCommit;
  }

  void shutdown() {
    retryScheduler.shutdown();
  }

  @Override
  public void lookupTopLevelTargets(
      long evaluatingVersion,
      String configurationHash,
      boolean useFakeStampData,
      String bazelVersion,
      EventHandler eventHandler,
      Runnable bailOutCallback)
      throws InterruptedException {
    if (metadataParams == null) {
      matchStatus.set(TopLevelTargetsMatchStatus.MATCH_STATUS_FAILURE);
      eventHandler.handle(Event.warn("Skycache: metadata parameters unavailable"));
      bailOutCallback.run();
      return;
    }

    GitRepositoryState gitState = getGitRepositoryState();
    QueryTopLevelTargetsRequest.Builder requestBuilder =
        QueryTopLevelTargetsRequest.newBuilder()
            .setBuild(
                buildMetadata(
                    evaluatingVersion, configurationHash, useFakeStampData, bazelVersion, gitState))
            .addAllTargets(metadataParams.getTargets())
            .addAllConfigFlags(metadataParams.getConfigFlags());
    if (gitState != null) {
      requestBuilder.addAllGitCommitCandidates(gitState.commitCandidates());
    }
    QueryTopLevelTargetsRequest request = requestBuilder.build();

    try {
      QueryTopLevelTargetsResponse response =
          blockingStub
              .withDeadlineAfter(SkycacheMetadataParams.TIMEOUT.toMillis(), MILLISECONDS)
              .queryTopLevelTargets(request);
      TopLevelTargetsMatchStatus status = response.getMatchStatus();
      matchStatus.set(status);
      maybeUpdateBaseCommit(response, gitState, eventHandler);
      switch (status) {
        case MATCH_STATUS_MATCH -> eventHandler.handle(Event.info("Skycache: metadata match"));
        case MATCH_STATUS_NO_MATCH -> {
          eventHandler.handle(Event.warn("Skycache: no cached targets found"));
          bailOutCallback.run();
        }
        case MATCH_STATUS_DIFFERENT_CL -> {
          eventHandler.handle(Event.warn("Skycache: cached targets from a different version"));
          bailOutCallback.run();
        }
        case MATCH_STATUS_DIFFERENT_CONFIG -> {
          eventHandler.handle(Event.warn("Skycache: cached targets use a different configuration"));
          bailOutCallback.run();
        }
        case MATCH_STATUS_DIFFERENT_CL_AND_CONFIG -> {
          eventHandler.handle(
              Event.warn("Skycache: cached targets use a different version and configuration"));
          bailOutCallback.run();
        }
        case MATCH_STATUS_FAILURE, MATCH_STATUS_UNSPECIFIED -> {
          eventHandler.handle(
              Event.warn(
                  Strings.isNullOrEmpty(response.getMessage())
                      ? "Skycache: metadata lookup failed"
                      : response.getMessage()));
          bailOutCallback.run();
        }
      }
    } catch (StatusRuntimeException e) {
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      matchStatus.set(TopLevelTargetsMatchStatus.MATCH_STATUS_FAILURE);
      eventHandler.handle(
          Event.warn("Skycache: metadata lookup failed: " + Status.fromThrowable(e)));
      bailOutCallback.run();
    }
  }

  static BuildMetadata buildMetadata(
      long evaluatingVersion,
      String configurationHash,
      boolean useFakeStampData,
      String bazelVersion,
      @Nullable GitRepositoryState gitRepositoryState) {
    BuildMetadata.Builder builder =
        BuildMetadata.newBuilder()
            .setEvaluatingVersion(evaluatingVersion)
            .setConfigurationHash(configurationHash)
            .setUseFakeStampData(useFakeStampData)
            .setBazelVersion(bazelVersion);
    if (gitRepositoryState != null) {
      WorkspaceVersion workspaceVersion = buildWorkspaceVersion(gitRepositoryState);
      if (workspaceVersion != null) {
        builder.setWorkspaceVersion(workspaceVersion);
      }
    }
    return builder.build();
  }

  private AnalysisCacheLookupRequest maybeAugmentRequest(AnalysisCacheLookupRequest request) {
    GitRepositoryState gitState = getGitRepositoryState();
    if (gitState == null) {
      return request;
    }
    if (!request.hasKey()) {
      return request;
    }
    var key = request.getKey();
    if (!key.hasFrontierVersion()) {
      return request;
    }
    var frontier = key.getFrontierVersion();
    if (frontier.hasWorkspaceVersion()
        && frontier.getWorkspaceVersion().getVcsCase() != WorkspaceVersion.VcsCase.VCS_NOT_SET) {
      return request;
    }
    WorkspaceVersion workspaceVersion = buildWorkspaceVersion(gitState);
    if (workspaceVersion == null) {
      return request;
    }
    var updatedFrontier = frontier.toBuilder().setWorkspaceVersion(workspaceVersion).build();
    var updatedKey = key.toBuilder().setFrontierVersion(updatedFrontier).build();
    return request.toBuilder().setKey(updatedKey).build();
  }

  @Nullable
  private static WorkspaceVersion buildWorkspaceVersion(GitRepositoryState gitRepositoryState) {
    if (Strings.isNullOrEmpty(gitRepositoryState.commit())) {
      return null;
    }
    GitWorkspaceVersion.Builder gitBuilder =
        GitWorkspaceVersion.newBuilder()
            .setCommit(gitRepositoryState.commit())
            .setIsDirty(gitRepositoryState.isDirty());
    if (!Strings.isNullOrEmpty(gitRepositoryState.tree())) {
      gitBuilder.setTree(gitRepositoryState.tree());
    }
    if (!Strings.isNullOrEmpty(gitRepositoryState.originUrl())) {
      gitBuilder.setOriginUrl(gitRepositoryState.originUrl());
    }
    if (!Strings.isNullOrEmpty(gitRepositoryState.branch())) {
      gitBuilder.setBranch(gitRepositoryState.branch());
    }
    return WorkspaceVersion.newBuilder().setGit(gitBuilder.build()).build();
  }

  @Nullable
  private GitRepositoryState getGitRepositoryState() {
    return gitRepositoryStateRef == null ? null : gitRepositoryStateRef.get();
  }

  private void maybeUpdateBaseCommit(
      QueryTopLevelTargetsResponse response,
      @Nullable GitRepositoryState gitState,
      EventHandler eventHandler) {
    if (gitState == null || gitRepositoryStateRef == null) {
      return;
    }
    if (!response.hasSelectedGitCommit()) {
      return;
    }
    GitCommitCandidate selected = response.getSelectedGitCommit();
    if (Strings.isNullOrEmpty(selected.getCommit())) {
      return;
    }
    selectedCommit = selected.getCommit();
    GitRepositoryState updated = gitState.withBaseCommit(selected.getCommit(), selected.getTree());
    gitRepositoryStateRef.set(updated);
    eventHandler.handle(
        Event.info("Skycache: using analysis cache base commit " + selected.getCommit()));
  }

  private ListenableFuture<AnalysisCacheBatchLookupResponse> submitBatchLookup(
      AnalysisCacheBatchLookupRequest batchRequest) {
    if (concurrencyLimiter == null) {
      batches.incrementAndGet();
      return callWithRetry(batchRequest);
    }

    if (concurrencyLimiter.tryAcquire()) {
      batches.incrementAndGet();
      ListenableFuture<AnalysisCacheBatchLookupResponse> future = callWithRetry(batchRequest);
      future.addListener(concurrencyLimiter::release, directExecutor());
      return future;
    }

    SettableFuture<AnalysisCacheBatchLookupResponse> future = SettableFuture.create();
    retryScheduler.execute(
        () -> {
          try {
            concurrencyLimiter.acquire();
            batches.incrementAndGet();
            ListenableFuture<AnalysisCacheBatchLookupResponse> call = callWithRetry(batchRequest);
            Futures.addCallback(
                call,
                new com.google.common.util.concurrent.FutureCallback<>() {
                  @Override
                  public void onSuccess(AnalysisCacheBatchLookupResponse result) {
                    concurrencyLimiter.release();
                    future.set(result);
                  }

                  @Override
                  public void onFailure(Throwable t) {
                    concurrencyLimiter.release();
                    future.setException(t);
                  }
                },
                directExecutor());
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.setException(e);
          }
        });
    return future;
  }

  private ListenableFuture<AnalysisCacheBatchLookupResponse> callWithRetry(
      AnalysisCacheBatchLookupRequest batchRequest) {
    ListenableFuture<AnalysisCacheBatchLookupResponse> call =
        futureStub.withDeadlineAfter(deadline.toMillis(), MILLISECONDS).batchLookup(batchRequest);
    if (unreachableRetryInterval.isZero()) {
      return call;
    }
    return Futures.catchingAsync(
        call,
        Throwable.class,
        t -> {
          Status status = Status.fromThrowable(t);
          if (status.getCode() != Status.Code.UNAVAILABLE) {
            return Futures.immediateFailedFuture(t);
          }
          SettableFuture<AnalysisCacheBatchLookupResponse> retryFuture = SettableFuture.create();
          retryScheduler.schedule(
              () -> {
                ListenableFuture<AnalysisCacheBatchLookupResponse> retry =
                    futureStub
                        .withDeadlineAfter(deadline.toMillis(), MILLISECONDS)
                        .batchLookup(batchRequest);
                Futures.addCallback(
                    retry,
                    new com.google.common.util.concurrent.FutureCallback<>() {
                      @Override
                      public void onSuccess(AnalysisCacheBatchLookupResponse result) {
                        retryFuture.set(result);
                      }

                      @Override
                      public void onFailure(Throwable t) {
                        retryFuture.setException(t);
                      }
                    },
                    directExecutor());
              },
              unreachableRetryInterval.toMillis(),
              MILLISECONDS);
          return retryFuture;
        },
        directExecutor());
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
}
