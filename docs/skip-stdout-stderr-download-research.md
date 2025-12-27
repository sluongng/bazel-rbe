# Research: Skip Downloading stdout/stderr in Remote Execution

## Problem Statement

Currently, Bazel always downloads stdout/stderr from remote execution, even with `--remote_download_outputs=minimal`. This can be a significant overhead for test actions where stdout can be large.

## Key Findings

### 1. Current Architecture

**test.log creation flow:**
```
test-setup.sh (exec 2>&1) → spawn stdout → ActionResult.stdout_digest → download → FileOutErr → test.log file
```

**Key files:**
- `RemoteExecutionService.java:1336-1342` - Always downloads stdout/stderr
- `CombinedCache.java:648-684` - `downloadOutErr()` method
- `StandaloneTestStrategy.java:317-319` - Creates FileOutErr pointing to test.log path
- `tools/test/test-setup.sh:18` - `exec 2>&1` redirects test output to stdout

### 2. test.log is NOT a Spawn Output

- `test.log` is a declared **action output** (in `TestRunnerAction` constructor)
- But it's NOT in `getSpawnOutputs()` (line 370-397)
- test.log is created by downloading stdout and writing to the FileOutErr path
- This means: **if we skip downloading stdout, test.log will be empty**

### 3. Split XML Gen Action

When a test doesn't produce its own `test.xml`, Bazel runs a separate spawn:
- Defined in `StandaloneTestStrategy.createXmlGeneratingSpawn()` (line 450-486)
- Reads `test.log` file to generate `test.xml`
- Uses `tools/test/generate-xml.sh`
- The spawn uses `withOutputsAsInputs(ImmutableList.of(testAction.getTestLog()))` (line 861)

**Impact:** If test.log is empty (stdout not downloaded), XML generation will produce empty/incorrect results.

## Proposed Solution: Option 2 - Pass stdout Directly to XML Gen

### Approach

Instead of downloading stdout to create test.log locally:
1. Store the stdout digest from `ActionResult.getStdoutDigest()`
2. Inject as remote file metadata for test.log path using `RemoteActionFileSystem.injectRemoteFile()`
3. XML gen spawn accesses test.log from CAS directly (runs remotely)

### Key Changes

#### RemoteExecutionService.downloadOutputs() (~line 1336)

```java
boolean isTestAction = action.getSpawn().getMnemonic().equals("TestRunner");
boolean shouldInjectStdoutAsRemoteFile =
    isTestAction
    && result.getExitCode() == 0  // Only for successful tests
    && result.actionResult.hasStdoutDigest()
    && !needsLocalStdout(action);  // Not streaming output

if (shouldInjectStdoutAsRemoteFile) {
    Digest stdoutDigest = result.actionResult.getStdoutDigest();
    PathFragment testLogPath = getTestLogPath(action);
    remoteActionFileSystem.injectRemoteFile(
        testLogPath,
        DigestUtil.toBinaryDigest(stdoutDigest),
        stdoutDigest.getSizeBytes(),
        expirationTime);
} else {
    // Existing download logic
}
```

#### StandaloneTestStrategy.java (~line 826, 849)

Handle remote-only test.log:
```java
boolean testLogExists = fileOutErr.getOutputPath().exists()
    || checkRemoteTestLogExists(actionExecutionContext, testAction);
```

### Cases Where stdout MUST Still Be Downloaded

| Scenario | Reason |
|----------|--------|
| Exit code != 0 | Error output needed for debugging |
| `--test_output=streamed` | Output must be streamed to user |
| `--test_output=all/errors` | Output displayed after test |
| Local XML gen execution | File needs to be materialized |

### Files to Modify

1. `src/main/java/com/google/devtools/build/lib/remote/RemoteExecutionService.java`
2. `src/main/java/com/google/devtools/build/lib/exec/StandaloneTestStrategy.java`
3. `src/main/java/com/google/devtools/build/lib/remote/RemoteActionFileSystem.java` (if needed)

## Alternative Approaches Considered

### Option 1: Make test.log a Real Spawn Output

Modify `test-setup.sh` to redirect output to a file instead of stdout:
```bash
exec > "$TEST_LOG_FILE" 2>&1
```

**Pros:** Cleaner architecture, test.log stays in CAS naturally
**Cons:** Significant change to test infrastructure, affects all test rules

### Option 3: Special-Case Test Actions

Continue downloading stdout/stderr for test actions, only skip for other action types.

**Pros:** Minimal changes, safe
**Cons:** Doesn't solve the test stdout download problem

## Conclusion

Option 2 provides a good balance:
- Avoids downloading large test stdout
- Uses existing remote file metadata infrastructure
- XML gen spawn can access content from CAS
- Maintains compatibility with local execution when needed
