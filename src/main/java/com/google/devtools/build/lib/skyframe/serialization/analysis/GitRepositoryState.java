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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.flogger.GoogleLogger;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.devtools.build.lib.shell.Command;
import com.google.devtools.build.lib.shell.CommandException;
import com.google.devtools.build.lib.shell.CommandResult;
import com.google.devtools.build.lib.shell.TerminationStatus;
import com.google.devtools.build.lib.skyframe.serialization.analysis.proto.GitCommitCandidate;
import com.google.devtools.build.lib.util.CommandBuilder;
import com.google.devtools.build.lib.vfs.Path;
import com.google.errorprone.annotations.Immutable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/** Git repository metadata used for remote analysis caching. */
@Immutable
public final class GitRepositoryState {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private static final int MAX_CANDIDATE_COMMITS = 64;
  private static final int MERGE_BASE_PARENTS = 2;

  @Nullable private final String commit;
  @Nullable private final String tree;
  @Nullable private final String originUrl;
  @Nullable private final String branch;
  private final boolean dirty;
  private final long evaluatingVersion;
  @Nullable private final String workspaceId;
  @Nullable private final Integer commitCount;

  @SuppressWarnings("Immutable") // Path is effectively immutable for our usage.
  private final Path repoRoot;

  private final ImmutableMap<String, String> clientEnv;

  private GitRepositoryState(
      @Nullable String commit,
      @Nullable String tree,
      @Nullable String originUrl,
      @Nullable String branch,
      boolean dirty,
      long evaluatingVersion,
      @Nullable String workspaceId,
      @Nullable Integer commitCount,
      Path repoRoot,
      ImmutableMap<String, String> clientEnv) {
    this.commit = commit;
    this.tree = tree;
    this.originUrl = originUrl;
    this.branch = branch;
    this.dirty = dirty;
    this.evaluatingVersion = evaluatingVersion;
    this.workspaceId = workspaceId;
    this.commitCount = commitCount;
    this.repoRoot = repoRoot;
    this.clientEnv = clientEnv;
  }

  public static Optional<GitRepositoryState> maybeCreate(
      Path workspaceRoot, Map<String, String> clientEnv) {
    GitCommandRunner initialRunner = new GitCommandRunner(workspaceRoot, clientEnv);
    String repoRoot = initialRunner.runAndCapture("git", "rev-parse", "--show-toplevel");
    if (Strings.isNullOrEmpty(repoRoot)) {
      return Optional.empty();
    }

    Path repoRootPath = workspaceRoot.getFileSystem().getPath(repoRoot);
    ImmutableMap<String, String> clientEnvCopy = ImmutableMap.copyOf(clientEnv);
    GitCommandRunner runner = new GitCommandRunner(repoRootPath, clientEnvCopy);
    String commit = runner.runAndCapture("git", "rev-parse", "HEAD");
    if (Strings.isNullOrEmpty(commit)) {
      return Optional.empty();
    }

    String tree = runner.runAndCapture("git", "rev-parse", "HEAD^{tree}");
    String originUrl = runner.runAndCapture("git", "config", "--get", "remote.origin.url");
    String branch = runner.runAndCapture("git", "rev-parse", "--abbrev-ref", "HEAD");
    if ("HEAD".equals(branch)) {
      branch = null;
    }

    boolean shallow =
        "true".equals(runner.runAndCapture("git", "rev-parse", "--is-shallow-repository"));

    Integer commitCount = null;
    if (!shallow) {
      String commitCountRaw = runner.runAndCapture("git", "rev-list", "--count", "HEAD");
      if (!Strings.isNullOrEmpty(commitCountRaw)) {
        try {
          long count = Long.parseLong(commitCountRaw.trim());
          if (count <= Integer.MAX_VALUE) {
            commitCount = (int) count;
          }
        } catch (NumberFormatException e) {
          logger.atFine().withCause(e).log("Failed to parse git commit count: %s", commitCountRaw);
        }
      }
    }

    byte[] statusBytes = runner.runAndCaptureBytes("git", "status", "--porcelain=v2", "-z");
    boolean dirty = statusBytes != null && statusBytes.length > 0;

    long evaluatingVersion =
        dirty
            ? computeDirtyVersionHash(runner, commit, statusBytes, /* diffBase= */ "HEAD")
            : evaluatingVersionForCommit(commit);

    String workspaceId =
        Hashing.murmur3_128()
            .hashString(
                !Strings.isNullOrEmpty(originUrl) ? originUrl : workspaceRoot.getPathString(),
                StandardCharsets.UTF_8)
            .toString();

    return Optional.of(
        new GitRepositoryState(
            commit,
            tree,
            originUrl,
            branch,
            dirty,
            evaluatingVersion,
            workspaceId,
            commitCount,
            repoRootPath,
            clientEnvCopy));
  }

  public long evaluatingVersion() {
    return evaluatingVersion;
  }

  /** Returns true if the working tree has uncommitted changes. */
  public boolean isDirty() {
    return dirty;
  }

  @Nullable
  public String commit() {
    return commit;
  }

  @Nullable
  public String tree() {
    return tree;
  }

  @Nullable
  public String originUrl() {
    return originUrl;
  }

  @Nullable
  public String branch() {
    return branch;
  }

  @Nullable
  public String workspaceId() {
    return workspaceId;
  }

  @Nullable
  public Integer commitCount() {
    return commitCount;
  }

  public ImmutableList<GitCommitCandidate> commitCandidates() {
    GitCommandRunner runner = new GitCommandRunner(repoRoot, clientEnv);
    return computeCommitCandidates(runner);
  }

  public GitRepositoryState withBaseCommit(String baseCommit, @Nullable String baseTree) {
    if (Strings.isNullOrEmpty(baseCommit)) {
      return this;
    }
    if (baseCommit.equals(commit) && (baseTree == null || baseTree.equals(tree))) {
      return this;
    }
    GitCommandRunner runner = new GitCommandRunner(repoRoot, clientEnv);
    String treeForBase = baseTree;
    if (Strings.isNullOrEmpty(treeForBase)) {
      treeForBase = runner.runAndCapture("git", "rev-parse", baseCommit + "^{tree}");
    }
    byte[] statusBytes = runner.runAndCaptureBytes("git", "status", "--porcelain=v2", "-z");
    boolean dirty = statusBytes != null && statusBytes.length > 0;
    boolean differsFromBase = isDirtyAgainstBase(runner, baseCommit, statusBytes);

    boolean shallow =
        "true".equals(runner.runAndCapture("git", "rev-parse", "--is-shallow-repository"));
    Integer baseCommitCount = null;
    if (!shallow) {
      String commitCountRaw = runner.runAndCapture("git", "rev-list", "--count", baseCommit);
      if (!Strings.isNullOrEmpty(commitCountRaw)) {
        try {
          long count = Long.parseLong(commitCountRaw.trim());
          if (count <= Integer.MAX_VALUE) {
            baseCommitCount = (int) count;
          }
        } catch (NumberFormatException e) {
          logger.atFine().withCause(e).log("Failed to parse git commit count: %s", commitCountRaw);
        }
      }
    }

    long evaluatingVersion =
        differsFromBase
            ? computeDirtyVersionHash(runner, baseCommit, statusBytes, /* diffBase= */ baseCommit)
            : evaluatingVersionForCommit(baseCommit);

    return new GitRepositoryState(
        baseCommit,
        treeForBase,
        originUrl,
        branch,
        dirty,
        evaluatingVersion,
        workspaceId,
        baseCommitCount,
        repoRoot,
        clientEnv);
  }

  private static long computeDirtyVersionHash(
      GitCommandRunner runner, String commit, @Nullable byte[] statusBytes, String diffBase) {
    Hasher hasher = Hashing.murmur3_128().newHasher();
    hasher.putString(commit, StandardCharsets.UTF_8);
    hasher.putByte((byte) 0);
    byte[] status = statusBytes == null ? new byte[0] : statusBytes;
    if (!hashGitDiff(runner, hasher, diffBase)) {
      // Fall back to hashing status output if diff failed.
      hasher.putBytes(status);
      return hasher.hash().asLong();
    }

    for (String path : parseNullSeparated(status)) {
      if (!path.startsWith("? ")) {
        continue;
      }
      String filePath = path.substring(2);
      hasher.putByte((byte) 1);
      hasher.putString(filePath, StandardCharsets.UTF_8);
      hasher.putByte((byte) 0);
      if (!hashFileContents(runner.workspaceRoot.getRelative(filePath), hasher)) {
        hasher.putString(filePath, StandardCharsets.UTF_8);
      }
    }
    return hasher.hash().asLong();
  }

  public static long evaluatingVersionForCommit(String commit) {
    return Hashing.murmur3_128().hashString(commit, StandardCharsets.UTF_8).asLong();
  }

  private static boolean hashGitDiff(GitCommandRunner runner, Hasher hasher, String diffBase) {
    try (HasherOutputStream hasherStream = new HasherOutputStream(hasher)) {
      return runner.runToStream(
          hasherStream, "git", "diff", "--no-ext-diff", "--binary", "--full-index", diffBase);
    } catch (IOException e) {
      logger.atFine().withCause(e).log("Failed to hash git diff output");
      return false;
    }
  }

  private static boolean isDirtyAgainstBase(
      GitCommandRunner runner, String baseCommit, @Nullable byte[] statusBytes) {
    byte[] diffBytes =
        runner.runAndCaptureBytes("git", "diff", "--name-only", "--no-ext-diff", baseCommit);
    if (diffBytes != null && diffBytes.length > 0) {
      return true;
    }
    return statusBytes != null && statusBytes.length > 0;
  }

  private static ImmutableList<GitCommitCandidate> computeCommitCandidates(
      GitCommandRunner runner) {
    String head = runner.runAndCapture("git", "rev-parse", "HEAD");
    if (Strings.isNullOrEmpty(head)) {
      return ImmutableList.of();
    }

    String defaultBranch = resolveDefaultBranch(runner);
    String mergeBase = null;
    if (!Strings.isNullOrEmpty(defaultBranch)) {
      mergeBase = runner.runAndCapture("git", "merge-base", "HEAD", defaultBranch);
    }

    List<String> commits = new ArrayList<>();
    if (!Strings.isNullOrEmpty(mergeBase)) {
      List<String> revs =
          runner.runAndCaptureLines(
              "git",
              "rev-list",
              "--first-parent",
              "-n",
              String.valueOf(MAX_CANDIDATE_COMMITS),
              "HEAD");
      for (String rev : revs) {
        commits.add(rev);
        if (rev.equals(mergeBase) || commits.size() >= MAX_CANDIDATE_COMMITS) {
          break;
        }
      }
      if (!commits.contains(mergeBase)) {
        if (commits.size() >= MAX_CANDIDATE_COMMITS) {
          commits.remove(commits.size() - 1);
        }
        commits.add(mergeBase);
      }
      String parent = mergeBase;
      for (int i = 0; i < MERGE_BASE_PARENTS && commits.size() < MAX_CANDIDATE_COMMITS; i++) {
        parent = runner.runAndCapture("git", "rev-parse", parent + "^");
        if (Strings.isNullOrEmpty(parent)) {
          break;
        }
        commits.add(parent);
      }
    } else {
      commits.addAll(
          runner.runAndCaptureLines(
              "git",
              "rev-list",
              "--first-parent",
              "-n",
              String.valueOf(MAX_CANDIDATE_COMMITS),
              "HEAD"));
    }

    LinkedHashSet<String> uniqueCommits = new LinkedHashSet<>(commits);
    ImmutableList.Builder<GitCommitCandidate> candidates = ImmutableList.builder();
    for (String commit : uniqueCommits) {
      if (Strings.isNullOrEmpty(commit)) {
        continue;
      }
      GitCommitCandidate.Builder candidate = GitCommitCandidate.newBuilder().setCommit(commit);
      String tree = runner.runAndCapture("git", "rev-parse", commit + "^{tree}");
      if (!Strings.isNullOrEmpty(tree)) {
        candidate.setTree(tree);
      }
      candidates.add(candidate.build());
    }
    return candidates.build();
  }

  @Nullable
  private static String resolveDefaultBranch(GitCommandRunner runner) {
    String originHead =
        runner.runAndCapture("git", "symbolic-ref", "-q", "refs/remotes/origin/HEAD");
    if (!Strings.isNullOrEmpty(originHead)) {
      return originHead;
    }
    if (refExists(runner, "refs/remotes/origin/main")) {
      return "refs/remotes/origin/main";
    }
    if (refExists(runner, "refs/remotes/origin/master")) {
      return "refs/remotes/origin/master";
    }
    return null;
  }

  private static boolean refExists(GitCommandRunner runner, String ref) {
    return !Strings.isNullOrEmpty(runner.runAndCapture("git", "rev-parse", "--verify", ref));
  }

  private static boolean hashFileContents(Path path, Hasher hasher) {
    try (var input = path.getInputStream()) {
      byte[] buffer = new byte[8192];
      int read;
      while ((read = input.read(buffer)) > 0) {
        hasher.putBytes(buffer, 0, read);
      }
      return true;
    } catch (IOException e) {
      logger.atFine().withCause(e).log("Failed to hash file contents for %s", path);
      return false;
    }
  }

  private static List<String> parseNullSeparated(byte[] bytes) {
    List<String> entries = new ArrayList<>();
    int start = 0;
    for (int i = 0; i < bytes.length; i++) {
      if (bytes[i] == 0) {
        if (i > start) {
          entries.add(new String(bytes, start, i - start, StandardCharsets.UTF_8));
        }
        start = i + 1;
      }
    }
    if (start < bytes.length) {
      entries.add(new String(bytes, start, bytes.length - start, StandardCharsets.UTF_8));
    }
    return entries;
  }

  private static final class GitCommandRunner {
    private final Path workspaceRoot;
    private final Map<String, String> clientEnv;

    private GitCommandRunner(Path workspaceRoot, Map<String, String> clientEnv) {
      this.workspaceRoot = workspaceRoot;
      this.clientEnv = clientEnv;
    }

    @Nullable
    private String runAndCapture(String... args) {
      byte[] bytes = runAndCaptureBytes(args);
      if (bytes == null || bytes.length == 0) {
        return null;
      }
      String output = new String(bytes, StandardCharsets.UTF_8).trim();
      return output.isEmpty() ? null : output;
    }

    @Nullable
    private byte[] runAndCaptureBytes(String... args) {
      try {
        CommandResult result = buildCommand(args).execute();
        TerminationStatus status = result.terminationStatus();
        if (!status.success()) {
          logger.atFine().log(
              "Git command failed (%s): %s", status.getRawExitCode(), String.join(" ", args));
          return null;
        }
        return result.getStdout();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.atFine().withCause(e).log(
            "Interrupted while running git command: %s", String.join(" ", args));
        return null;
      } catch (CommandException e) {
        logger.atFine().withCause(e).log("Failed to run git command: %s", String.join(" ", args));
        return null;
      }
    }

    private List<String> runAndCaptureLines(String... args) {
      byte[] bytes = runAndCaptureBytes(args);
      if (bytes == null || bytes.length == 0) {
        return ImmutableList.of();
      }
      String output = new String(bytes, StandardCharsets.UTF_8).trim();
      if (output.isEmpty()) {
        return ImmutableList.of();
      }
      return ImmutableList.copyOf(output.split("\\R"));
    }

    private boolean runToStream(OutputStream outputStream, String... args) {
      try {
        CommandResult result =
            buildCommand(args)
                .execute(
                    outputStream,
                    // Discard stderr to avoid buffering large output.
                    new ByteArrayOutputStream());
        return result.terminationStatus().success();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.atFine().withCause(e).log(
            "Interrupted while running git command: %s", String.join(" ", args));
        return false;
      } catch (CommandException e) {
        logger.atFine().withCause(e).log("Failed to run git command: %s", String.join(" ", args));
        return false;
      }
    }

    private Command buildCommand(String... args) {
      return new CommandBuilder(clientEnv).addArgs(args).setWorkingDir(workspaceRoot).build();
    }
  }

  private static final class HasherOutputStream extends OutputStream implements AutoCloseable {
    private final Hasher hasher;

    private HasherOutputStream(Hasher hasher) {
      this.hasher = hasher;
    }

    @Override
    public void write(int b) {
      hasher.putByte((byte) b);
    }

    @Override
    public void write(byte[] b, int off, int len) {
      hasher.putBytes(b, off, len);
    }
  }
}
