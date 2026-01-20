#!/usr/bin/env bash
#
# Copyright 2025 The Bazel Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Tests remote analysis cache usage.

set -euo pipefail

# --- begin runfiles.bash initialization v3 ---
# Copy-pasted from the Bazel Bash runfiles library v3.
set -uo pipefail; set +e; f=bazel_tools/tools/bash/runfiles/runfiles.bash
source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null || \
source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" | cut -f2- -d' ')" 2>/dev/null || \
source "$0.runfiles/$f" 2>/dev/null || \
source "$(grep -sm1 "^$f " "$0.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
source "$(grep -sm1 "^$f " "$0.exe.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
{ echo>&2 "ERROR: cannot find $f"; exit 1; }; f=; set -e
# --- end runfiles.bash initialization v3 ---

source "$(rlocation "io_bazel/src/test/shell/integration_test_setup.sh")" \
  || { echo "integration_test_setup.sh not found!" >&2; exit 1; }
source "$(rlocation "io_bazel/src/test/shell/bazel/remote/remote_utils.sh")" \
  || { echo "remote_utils.sh not found!" >&2; exit 1; }

function set_up() {
  setup_clean_workspace
  analysis_cache_log="${TEST_TMPDIR}/analysis_cache_server.log"
  start_worker --analysis_cache_log_path="${analysis_cache_log}"
}

function tear_down() {
  bazel clean >& $TEST_log || true
  stop_worker
}

function write_simple_workspace() {
  mkdir -p foo
  cat > foo/BUILD <<'EOF'
genrule(
    name = "foo",
    outs = ["foo.out"],
    cmd = "echo hi > $@",
)
EOF
}

function init_git_repo() {
  command -v git >/dev/null 2>&1 || fail "git is required for this test"
  git init >/dev/null
  git config user.email "analysis-cache@test.invalid"
  git config user.name "Analysis Cache Test"
  cat > .gitignore <<'EOF'
bazel-*
tools/
MODULE.bazel.lock
EOF
  mkdir -p foo
  cat > foo/BUILD <<'EOF'
genrule(
    name = "foo",
    outs = ["foo.out"],
    cmd = "echo hi > $@",
)
EOF
  cat > foo/input.txt <<'EOF'
hello
EOF
  mkdir -p bar
  cat > bar/BUILD <<'EOF'
genrule(
    name = "bar",
    outs = ["bar.out"],
    cmd = "echo hi > $@",
)
EOF
  cat > bar/input.txt <<'EOF'
hello
EOF
  git add .gitignore MODULE.bazel foo/BUILD foo/input.txt bar/BUILD bar/input.txt
  git commit -m "init" >/dev/null
  git checkout -b analysis-cache-test >/dev/null
  git remote add origin "https://example.com/repo.git"
}

function assert_log_has_git_metadata() {
  local commit="$1"
  local tree="$2"
  local dirty="$3"
  local branch="$4"
  local origin="$5"
  grep -Fq "git_commit=${commit}" "${analysis_cache_log}" \
      || fail "expected git commit in analysis cache log"
  grep -Fq "git_tree=${tree}" "${analysis_cache_log}" \
      || fail "expected git tree in analysis cache log"
  grep -Fq "git_branch=${branch}" "${analysis_cache_log}" \
      || fail "expected git branch in analysis cache log"
  grep -Fq "git_origin=${origin}" "${analysis_cache_log}" \
      || fail "expected git origin in analysis cache log"
  grep -Fq "git_dirty=${dirty}" "${analysis_cache_log}" \
      || fail "expected git dirty=${dirty} in analysis cache log"
}

function test_remote_analysis_cache_download() {
  write_simple_workspace

  : > "${analysis_cache_log}"
  bazel build \
      --experimental_remote_analysis_cache_mode=download \
      --experimental_analysis_cache_enable_metadata_queries=true \
      --experimental_analysis_cache_service="grpc://localhost:${worker_port}" \
      //foo:foo >& $TEST_log || fail "download build failed"
  grep -q "QUERY_TOP_LEVEL_TARGETS" "${analysis_cache_log}" \
      || fail "expected QUERY_TOP_LEVEL_TARGETS in analysis cache server log"
  grep -q "BATCH_LOOKUP" "${analysis_cache_log}" \
      || fail "expected BATCH_LOOKUP in analysis cache server log"
}

function test_remote_analysis_cache_git_clean() {
  init_git_repo

  : > "${analysis_cache_log}"
  bazel build \
      --experimental_remote_analysis_cache_mode=download \
      --experimental_analysis_cache_enable_metadata_queries=true \
      --experimental_analysis_cache_service="grpc://localhost:${worker_port}" \
      //foo:foo >& $TEST_log || fail "git clean build failed"

  local commit
  local tree
  commit="$(git rev-parse HEAD)"
  tree="$(git rev-parse HEAD^{tree})"
  assert_log_has_git_metadata "${commit}" "${tree}" "false" "analysis-cache-test" \
      "https://example.com/repo.git"
}

function test_remote_analysis_cache_git_dirty() {
  init_git_repo
  echo "dirty" >> foo/input.txt

  : > "${analysis_cache_log}"
  bazel build \
      --experimental_remote_analysis_cache_mode=download \
      --experimental_analysis_cache_enable_metadata_queries=true \
      --experimental_analysis_cache_service="grpc://localhost:${worker_port}" \
      //foo:foo >& $TEST_log || fail "git dirty build failed"

  local commit
  local tree
  commit="$(git rev-parse HEAD)"
  tree="$(git rev-parse HEAD^{tree})"
  assert_log_has_git_metadata "${commit}" "${tree}" "true" "analysis-cache-test" \
      "https://example.com/repo.git"
}

# Verifies base-commit cache hits and invalidation after a new commit is introduced.
# Repo layout in this test:
# $TEST_TMPDIR
# ├── workspace/                  (initial working repo, commit A)
# ├── analysis-cache-repo.git/    (bare repo used by worker for diffs)
# └── workspace.base-commit/      (second working repo, commit B)
function test_remote_analysis_cache_base_commit_invalidation() {
  # Initialize a git workspace and record the base commit.
  init_git_repo
  local base_commit="$(git rev-parse HEAD)"

  # Create a bare repo and restart the worker to serve base-commit data + diffs.
  local bare_repo="${TEST_TMPDIR}/analysis-cache-repo.git"
  rm -rf "${bare_repo}"
  git clone --bare . "${bare_repo}" >/dev/null
  stop_worker
  start_worker \
      --analysis_cache_log_path="${analysis_cache_log}" \
      --analysis_cache_available_commits="${base_commit}" \
      --analysis_cache_git_repo_path="${bare_repo}"

  # Upload analysis cache entries for the base commit.
  local upload_log="${TEST_TMPDIR}/analysis_cache_upload.log"
  : > "${analysis_cache_log}"
  bazel build --nobuild \
      --experimental_remote_analysis_cache_mode=upload \
      --experimental_analysis_cache_enable_metadata_queries=true \
      --experimental_analysis_cache_service="grpc://localhost:${worker_port}" \
      //foo:foo //bar:bar >& "${upload_log}" || {
        cat "${upload_log}" >> "$TEST_log"
        fail "git base commit upload failed"
      }
  grep -Fq "WRITE_TOP_LEVEL_TARGETS_WORKSPACE git_commit=${base_commit}" "${analysis_cache_log}" \
      || fail "expected base commit metadata write in analysis cache log"
  grep -Fq "PUT_VALUES" "${analysis_cache_log}" \
      || fail "expected analysis cache values to be uploaded"

  # Recreate the workspace at base commit, then add a new commit and push it.
  local original_workspace="${PWD}"
  local new_workspace="${TEST_TMPDIR}/workspace.base-commit"
  cd "${TEST_TMPDIR}"
  rm -rf "${original_workspace}" "${new_workspace}"
  git clone "${bare_repo}" "${new_workspace}" >/dev/null
  workspaces+=("${new_workspace}")
  cd "${new_workspace}"
  export WORKSPACE_DIR="${new_workspace}"
  mkdir -p tools
  copy_tools_directory
  cp -f $(rlocation io_bazel/src/test/tools/bzlmod/MODULE.bazel.lock) MODULE.bazel.lock
  git config user.email "analysis-cache@test.invalid"
  git config user.name "Analysis Cache Test"
  cat > foo/BUILD <<'EOF'
genrule(
    name = "foo",
    outs = ["foo.out"],
    cmd = "echo hi2 > $@",
)
EOF
  git add foo/BUILD
  git commit -m "second" >/dev/null
  git push origin HEAD >/dev/null

  # Download analysis cache and verify base-commit selection.
  local download_log="${TEST_TMPDIR}/analysis_cache_download.log"
  : > "${analysis_cache_log}"
  bazel build --nobuild \
      --experimental_remote_analysis_cache_mode=download \
      --experimental_analysis_cache_enable_metadata_queries=true \
      --experimental_analysis_cache_service="grpc://localhost:${worker_port}" \
      //foo:foo //bar:bar >& "${download_log}" || {
        cat "${download_log}" >> "$TEST_log"
        fail "git base commit download failed"
      }
  grep -Fq "QUERY_TOP_LEVEL_TARGETS_SELECTED git_commit=${base_commit}" "${analysis_cache_log}" \
      || fail "expected selected base commit in analysis cache log"
  grep -Fq "Skycache: using analysis cache base commit ${base_commit}" "${download_log}" \
      || fail "expected base commit selection in bazel output"

  # Assert mixed hits/misses after the base commit diff is applied.
  local stats_line
  stats_line="$(grep -F "Skycache stats:" "${download_log}" | tail -n 1 || true)"
  [[ -n "${stats_line}" ]] || fail "expected Skycache stats in bazel output"
  if [[ "${stats_line}" =~ ([0-9]+)/([0-9]+)\ cache\ hits ]]; then
    local hits="${BASH_REMATCH[1]}"
    local total="${BASH_REMATCH[2]}"
    if (( hits == 0 )); then
      fail "expected cache hits from base commit; stats: ${stats_line}"
    fi
    if (( hits == total )); then
      fail "expected some cache misses due to changes since base commit; stats: ${stats_line}"
    fi
  else
    fail "failed to parse Skycache stats line: ${stats_line}"
  fi
}

run_suite "remote analysis cache tests"
