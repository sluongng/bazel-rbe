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
package com.google.devtools.build.lib.util;

import static java.lang.invoke.MethodType.methodType;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

/** Blocker wrapper for jdk.internal.misc.Blocker. */
public final class Blocker {

  public static Object begin() {
    if (IN_NATIVE_IMAGE) {
      return null;
    }
    try {
      return BEGIN.invoke();
    } catch (Throwable e) {
      throw new LinkageError(e.getMessage(), e);
    }
  }

  public static void end(Object comp) {
    if (IN_NATIVE_IMAGE) {
      return;
    }
    try {
      END.invoke(comp);
    } catch (Throwable e) {
      throw new LinkageError(e.getMessage(), e);
    }
  }

  private static final boolean IN_NATIVE_IMAGE =
      System.getProperty("org.graalvm.nativeimage.imagecode") != null;

  private static final MethodHandle BEGIN = IN_NATIVE_IMAGE ? null : getBegin();

  private static final MethodHandle END = IN_NATIVE_IMAGE ? null : getEnd();

  private static Class<?> blockerType() {
    return Runtime.version().feature() >= 23 ? boolean.class : long.class;
  }

  private static MethodHandle getEnd() {
    try {
      return MethodHandles.lookup()
          .findStatic(
              jdk.internal.misc.Blocker.class, "end", methodType(void.class, blockerType()));
    } catch (ReflectiveOperationException e) {
      throw new LinkageError(e.getMessage(), e);
    }
  }

  private static MethodHandle getBegin() {
    try {
      return MethodHandles.lookup()
          .findStatic(jdk.internal.misc.Blocker.class, "begin", methodType(blockerType()));
    } catch (ReflectiveOperationException e) {
      throw new LinkageError(e.getMessage(), e);
    }
  }

  private Blocker() {}
}
