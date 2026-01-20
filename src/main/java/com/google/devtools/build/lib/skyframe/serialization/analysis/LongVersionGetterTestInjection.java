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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.devtools.build.lib.util.TestType.isInTest;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.devtools.build.lib.vfs.Dirent;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.Symlinks;
import com.google.devtools.build.lib.versioning.LongVersionGetter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Allows injecting a {@link LongVersionGetter} implementation to {@link FrontierSerializer} in
 * tests.
 */
@SuppressWarnings("NonFinalStaticField")
public final class LongVersionGetterTestInjection {
  private static LongVersionGetter versionGetter = null;
  private static boolean wasAccessed = false;

  static LongVersionGetter getVersionGetterForTesting() {
    checkState(isInTest());
    wasAccessed = true;
    if (versionGetter == null) {
      versionGetter = new ContentHashVersionGetter();
    }
    return checkNotNull(versionGetter, "injectVersionGetterForTesting must be called first");
  }

  public static void injectVersionGetterForTesting(LongVersionGetter versionGetter) {
    checkState(isInTest());
    LongVersionGetterTestInjection.versionGetter = versionGetter;
  }

  public static boolean wasGetterAccessed() {
    return wasAccessed;
  }

  private static final class ContentHashVersionGetter implements LongVersionGetter {
    @Override
    public long getFilePathOrSymlinkVersion(Path path) {
      if (!path.exists()) {
        return LongVersionGetter.MINIMAL;
      }
      try {
        return hashFileContents(path);
      } catch (IOException e) {
        return LongVersionGetter.CURRENT_VERSION;
      }
    }

    @Override
    public long getDirectoryListingVersion(Path path) {
      if (!path.exists()) {
        return LongVersionGetter.MINIMAL;
      }
      try {
        return hashDirectoryListing(path);
      } catch (IOException e) {
        return LongVersionGetter.CURRENT_VERSION;
      }
    }

    @Override
    public long getNonexistentPathVersion(Path path) {
      return LongVersionGetter.MINIMAL;
    }

    private static long hashFileContents(Path path) throws IOException {
      Hasher hasher = Hashing.murmur3_128().newHasher();
      try (InputStream in = path.getInputStream()) {
        byte[] buffer = new byte[8192];
        int read;
        while ((read = in.read(buffer)) > 0) {
          hasher.putBytes(buffer, 0, read);
        }
      }
      return hasher.hash().asLong() & Long.MAX_VALUE;
    }

    private static long hashDirectoryListing(Path path) throws IOException {
      List<String> names = new ArrayList<>();
      for (Dirent entry : path.readdir(Symlinks.FOLLOW)) {
        names.add(entry.getName());
      }
      Collections.sort(names);
      Hasher hasher = Hashing.murmur3_128().newHasher();
      for (String name : names) {
        hasher.putString(name, StandardCharsets.UTF_8).putByte((byte) 0);
      }
      return hasher.hash().asLong() & Long.MAX_VALUE;
    }
  }

  private LongVersionGetterTestInjection() {}
}
