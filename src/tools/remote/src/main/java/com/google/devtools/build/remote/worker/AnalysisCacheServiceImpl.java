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
package com.google.devtools.build.remote.worker;

import com.google.common.hash.Hashing;
import com.google.devtools.build.lib.shell.Command;
import com.google.devtools.build.lib.shell.CommandException;
import com.google.devtools.build.lib.shell.CommandResult;
import com.google.devtools.build.lib.skyframe.serialization.proto.DataType;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisCacheBatchLookupRequest;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisCacheBatchLookupResponse;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisCacheKey;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisCacheLookupRequest;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisCacheLookupResponse;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisCacheServiceGrpc;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.AnalysisValue;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.BuildMetadata;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.FingerprintKey;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.GetValuesRequest;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.GetValuesResponse;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.GitCommitCandidate;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.GitWorkspaceVersion;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.KeyValue;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.LookupResult;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.PutValueResult;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.PutValuesRequest;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.PutValuesResponse;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.QueryTopLevelTargetsRequest;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.QueryTopLevelTargetsResponse;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.TopLevelTargetsMatchStatus;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.WorkspaceVersion;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.WriteTopLevelTargetsRequest;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.WriteTopLevelTargetsResponse;
import com.google.devtools.build.lib.vfs.Path;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

/** Simple AnalysisCacheService implementation for remote worker tests. */
final class AnalysisCacheServiceImpl extends AnalysisCacheServiceGrpc.AnalysisCacheServiceImplBase {
  private final Map<String, byte[]> values = new ConcurrentHashMap<>();
  @Nullable private final Path logPath;
  private final TopLevelTargetsMatchStatus matchStatus;
  private final Set<String> availableCommits;
  @Nullable private final Path gitRepoPath;
  private volatile Set<String> invalidatedPackages = Set.of();

  AnalysisCacheServiceImpl(
      @Nullable Path logPath,
      TopLevelTargetsMatchStatus matchStatus,
      Set<String> availableCommits,
      @Nullable Path gitRepoPath) {
    this.logPath = logPath;
    this.matchStatus = matchStatus;
    this.availableCommits = availableCommits;
    this.gitRepoPath = gitRepoPath;
    if (logPath != null) {
      try {
        logPath.getParentDirectory().createDirectoryAndParents();
      } catch (IOException e) {
        System.err.println("Failed to create analysis cache log directory: " + e.getMessage());
      }
    }
  }

  @Override
  public void batchLookup(
      AnalysisCacheBatchLookupRequest request,
      StreamObserver<AnalysisCacheBatchLookupResponse> responseObserver) {
    log("BATCH_LOOKUP " + request.getRequestsCount());
    logBatchWorkspaceVersion(request);
    List<AnalysisCacheLookupResponse> responses = new ArrayList<>(request.getRequestsCount());
    for (AnalysisCacheLookupRequest lookup : request.getRequestsList()) {
      AnalysisCacheKey key = lookup.getKey();
      String fingerprint = key.getFingerprintHex();
      byte[] value = values.get(fingerprint);
      AnalysisCacheLookupResponse.Builder response =
          AnalysisCacheLookupResponse.newBuilder().setKey(key);
      if (value == null) {
        response.setResult(LookupResult.LOOKUP_RESULT_MISS);
      } else if (shouldInvalidate(key)) {
        response.setResult(LookupResult.LOOKUP_RESULT_MISS);
      } else {
        byte[] responseValue = stripInvalidationDataIfPresent(value);
        response
            .setResult(LookupResult.LOOKUP_RESULT_HIT)
            .setValue(
                AnalysisValue.newBuilder()
                    .setSerializedValue(ByteString.copyFrom(responseValue))
                    .setSizeBytes(responseValue.length)
                    .setValueDigestHex(hashBytes(responseValue)));
      }
      responses.add(response.build());
    }
    responseObserver.onNext(
        AnalysisCacheBatchLookupResponse.newBuilder().addAllResponses(responses).build());
    responseObserver.onCompleted();
  }

  @Override
  public void queryTopLevelTargets(
      QueryTopLevelTargetsRequest request,
      StreamObserver<QueryTopLevelTargetsResponse> responseObserver) {
    log("QUERY_TOP_LEVEL_TARGETS " + request.getTargetsCount());
    if (request.hasBuild()) {
      logBuildWorkspaceVersion("QUERY_TOP_LEVEL_TARGETS_WORKSPACE", request.getBuild());
    }
    if (!request.getGitCommitCandidatesList().isEmpty()) {
      log("QUERY_TOP_LEVEL_TARGETS_CANDIDATES " + request.getGitCommitCandidatesCount());
      for (GitCommitCandidate candidate : request.getGitCommitCandidatesList()) {
        logGitCommitCandidate("QUERY_TOP_LEVEL_TARGETS_CANDIDATE", candidate);
      }
    }
    QueryTopLevelTargetsResponse.Builder response =
        QueryTopLevelTargetsResponse.newBuilder().setMatchStatus(matchStatus);
    GitCommitCandidate selected = selectCandidate(request.getGitCommitCandidatesList());
    if (selected != null) {
      response.setSelectedGitCommit(selected);
      logGitCommitCandidate("QUERY_TOP_LEVEL_TARGETS_SELECTED", selected);
    }
    updateInvalidatedPackages(request, selected);
    responseObserver.onNext(response.build());
    responseObserver.onCompleted();
  }

  @Override
  public void writeTopLevelTargets(
      WriteTopLevelTargetsRequest request,
      StreamObserver<WriteTopLevelTargetsResponse> responseObserver) {
    log("WRITE_TOP_LEVEL_TARGETS " + request.getTargetsCount());
    if (request.hasBuild()) {
      logBuildWorkspaceVersion("WRITE_TOP_LEVEL_TARGETS_WORKSPACE", request.getBuild());
    }
    responseObserver.onNext(WriteTopLevelTargetsResponse.newBuilder().setOk(true).build());
    responseObserver.onCompleted();
  }

  @Override
  public void getValues(
      GetValuesRequest request, StreamObserver<GetValuesResponse> responseObserver) {
    log("GET_VALUES " + request.getKeysCount());
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
                .setValueDigestHex(hashBytes(value)));
      }
    }
    responseObserver.onNext(response.build());
    responseObserver.onCompleted();
  }

  @Override
  public void putValues(
      PutValuesRequest request, StreamObserver<PutValuesResponse> responseObserver) {
    log("PUT_VALUES " + request.getValuesCount());
    PutValuesResponse.Builder response = PutValuesResponse.newBuilder();
    for (KeyValue value : request.getValuesList()) {
      values.put(value.getKey().getFingerprintHex(), value.getValue().toByteArray());
      response.addResults(PutValueResult.newBuilder().setKey(value.getKey()).setOk(true).build());
    }
    responseObserver.onNext(response.build());
    responseObserver.onCompleted();
  }

  private void log(String message) {
    if (logPath == null) {
      return;
    }
    try (OutputStreamWriter writer =
        new OutputStreamWriter(logPath.getOutputStream(true), StandardCharsets.UTF_8)) {
      writer.write(message);
      writer.write(System.lineSeparator());
    } catch (IOException e) {
      System.err.println("Failed to write analysis cache log: " + e.getMessage());
    }
  }

  private void logBatchWorkspaceVersion(AnalysisCacheBatchLookupRequest request) {
    WorkspaceVersion workspaceVersion = null;
    for (AnalysisCacheLookupRequest lookup : request.getRequestsList()) {
      if (!lookup.hasKey()) {
        continue;
      }
      AnalysisCacheKey key = lookup.getKey();
      if (key.hasFrontierVersion() && key.getFrontierVersion().hasWorkspaceVersion()) {
        workspaceVersion = key.getFrontierVersion().getWorkspaceVersion();
        break;
      }
    }
    logWorkspaceVersion("BATCH_LOOKUP_WORKSPACE", workspaceVersion);
  }

  private void logBuildWorkspaceVersion(String prefix, BuildMetadata metadata) {
    if (metadata.hasWorkspaceVersion()) {
      logWorkspaceVersion(prefix, metadata.getWorkspaceVersion());
    }
  }

  private void logWorkspaceVersion(String prefix, @Nullable WorkspaceVersion workspaceVersion) {
    if (workspaceVersion == null) {
      return;
    }
    if (workspaceVersion.getVcsCase() != WorkspaceVersion.VcsCase.GIT) {
      log(prefix + " vcs=" + workspaceVersion.getVcsCase());
      return;
    }
    GitWorkspaceVersion git = workspaceVersion.getGit();
    StringBuilder message = new StringBuilder(prefix);
    if (!git.getCommit().isEmpty()) {
      message.append(" git_commit=").append(git.getCommit());
    }
    if (!git.getTree().isEmpty()) {
      message.append(" git_tree=").append(git.getTree());
    }
    if (!git.getBranch().isEmpty()) {
      message.append(" git_branch=").append(git.getBranch());
    }
    if (!git.getOriginUrl().isEmpty()) {
      message.append(" git_origin=").append(git.getOriginUrl());
    }
    message.append(" git_dirty=").append(git.getIsDirty());
    log(message.toString());
  }

  private void logGitCommitCandidate(String prefix, GitCommitCandidate candidate) {
    StringBuilder message = new StringBuilder(prefix);
    if (!candidate.getCommit().isEmpty()) {
      message.append(" git_commit=").append(candidate.getCommit());
    }
    if (!candidate.getTree().isEmpty()) {
      message.append(" git_tree=").append(candidate.getTree());
    }
    log(message.toString());
  }

  private void updateInvalidatedPackages(
      QueryTopLevelTargetsRequest request, @Nullable GitCommitCandidate selected) {
    if (gitRepoPath == null) {
      invalidatedPackages = Set.of();
      return;
    }
    if (selected == null || selected.getCommit().isEmpty()) {
      invalidatedPackages = Set.of();
      return;
    }
    String currentCommit = "";
    if (request.hasBuild()
        && request.getBuild().hasWorkspaceVersion()
        && request.getBuild().getWorkspaceVersion().getVcsCase() == WorkspaceVersion.VcsCase.GIT) {
      currentCommit = request.getBuild().getWorkspaceVersion().getGit().getCommit();
    }
    if (currentCommit.isEmpty() || currentCommit.equals(selected.getCommit())) {
      invalidatedPackages = Set.of();
      return;
    }
    invalidatedPackages = computeChangedPackages(selected.getCommit(), currentCommit);
  }

  private Set<String> computeChangedPackages(String baseCommit, String currentCommit) {
    if (gitRepoPath == null) {
      return Set.of();
    }
    List<String> commandLine = new ArrayList<>();
    commandLine.add("git");
    commandLine.add("--git-dir=" + gitRepoPath.getPathString());
    commandLine.add("diff");
    commandLine.add("--name-only");
    commandLine.add(baseCommit);
    commandLine.add(currentCommit);
    Command cmd = new Command(commandLine, null, gitRepoPath.getPathFile(), System.getenv());
    CommandResult result;
    try {
      result = cmd.execute();
    } catch (CommandException e) {
      log("INVALIDATION_DIFF_FAILED error=" + e.getMessage());
      return Set.of();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log("INVALIDATION_DIFF_FAILED error=interrupted");
      return Set.of();
    }
    if (!result.terminationStatus().success()) {
      log("INVALIDATION_DIFF_FAILED error=non_zero_exit");
      return Set.of();
    }
    String output = new String(result.getStdout(), StandardCharsets.UTF_8).trim();
    if (output.isEmpty()) {
      return Set.of();
    }
    Set<String> packages = new HashSet<>();
    for (String line : output.split("\\R")) {
      String path = line.trim();
      if (path.isEmpty()) {
        continue;
      }
      int slash = path.lastIndexOf('/');
      String pkg = slash > 0 ? path.substring(0, slash) : "";
      packages.add(pkg);
    }
    return Set.copyOf(packages);
  }

  private boolean shouldInvalidate(AnalysisCacheKey key) {
    Set<String> packages = invalidatedPackages;
    if (packages.isEmpty()) {
      return false;
    }
    String pkg = extractPackageFromSkyKey(key.getSkyKey(), key.getSkyFunction());
    return pkg != null && packages.contains(pkg);
  }

  @Nullable
  private static String extractPackageFromSkyKey(String skyKey, String skyFunction) {
    if (skyKey == null || skyKey.isEmpty()) {
      return null;
    }
    int labelIndex = skyKey.indexOf("label=");
    if (labelIndex < 0) {
      if (skyFunction == null || !skyFunction.contains("PACKAGE")) {
        return null;
      }
      return parsePackageString(skyKey);
    }
    int labelStart = labelIndex + "label=".length();
    int labelEnd = skyKey.indexOf(',', labelStart);
    if (labelEnd < 0) {
      labelEnd = skyKey.indexOf('}', labelStart);
    }
    if (labelEnd < 0) {
      labelEnd = skyKey.length();
    }
    String label = skyKey.substring(labelStart, labelEnd).trim();
    int repoSep = label.indexOf("//");
    if (repoSep < 0) {
      return null;
    }
    int pkgStart = repoSep + 2;
    int colon = label.indexOf(':', pkgStart);
    if (colon < 0) {
      return null;
    }
    return label.substring(pkgStart, colon);
  }

  @Nullable
  private static String parsePackageString(String skyKey) {
    String candidate = skyKey;
    int nameIndex = candidate.indexOf("name=");
    if (nameIndex >= 0) {
      int nameStart = nameIndex + "name=".length();
      int nameEnd = candidate.indexOf('>', nameStart);
      if (nameEnd < 0) {
        nameEnd = candidate.indexOf(' ', nameStart);
      }
      if (nameEnd < 0) {
        nameEnd = candidate.length();
      }
      candidate = candidate.substring(nameStart, nameEnd);
    } else {
      int colon = candidate.indexOf(':');
      if (colon >= 0) {
        candidate = candidate.substring(colon + 1);
      }
    }
    candidate = candidate.trim();
    if (candidate.isEmpty()) {
      return null;
    }
    if (candidate.startsWith("@")) {
      int repoSep = candidate.indexOf("//");
      if (repoSep < 0) {
        return null;
      }
      candidate = candidate.substring(repoSep + 2);
    } else if (candidate.startsWith("//")) {
      candidate = candidate.substring(2);
    }
    int colon = candidate.indexOf(':');
    if (colon >= 0) {
      candidate = candidate.substring(0, colon);
    }
    return candidate;
  }

  private static byte[] stripInvalidationDataIfPresent(byte[] value) {
    if (value.length == 0) {
      return value;
    }
    CodedInputStream codedIn = CodedInputStream.newInstance(value);
    try {
      int dataTypeOrdinal = codedIn.readInt32();
      DataType dataType = DataType.forNumber(dataTypeOrdinal);
      if (dataType == null) {
        return value;
      }
      switch (dataType) {
        case DATA_TYPE_EMPTY:
          break;
        case DATA_TYPE_FILE:
        case DATA_TYPE_LISTING:
          codedIn.readString();
          break;
        case DATA_TYPE_ANALYSIS_NODE:
        case DATA_TYPE_EXECUTION_NODE:
          codedIn.readFixed64();
          codedIn.readFixed64();
          break;
        default:
          return value;
      }
      int consumed = codedIn.getTotalBytesRead();
      if (consumed <= 0 || consumed >= value.length) {
        return value;
      }
      return Arrays.copyOfRange(value, consumed, value.length);
    } catch (IOException e) {
      return value;
    }
  }

  @Nullable
  private GitCommitCandidate selectCandidate(List<GitCommitCandidate> candidates) {
    if (availableCommits.isEmpty()) {
      return null;
    }
    for (GitCommitCandidate candidate : candidates) {
      if (availableCommits.contains(candidate.getCommit())) {
        return candidate;
      }
    }
    return null;
  }

  private static String hashBytes(byte[] bytes) {
    return Hashing.sha256().hashBytes(bytes).toString();
  }
}
