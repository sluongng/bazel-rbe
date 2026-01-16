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
import com.google.common.collect.ImmutableSet;
import com.google.devtools.build.lib.skyframe.serialization.SkycacheMetadataParams;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/** Simple implementation of {@link SkycacheMetadataParams}. */
public final class SkycacheMetadataParamsImpl implements SkycacheMetadataParams {
  private long evaluatingVersion;
  private String bazelVersion = "";
  private Collection<String> targets = ImmutableList.of();
  private boolean useFakeStampData;
  private ImmutableMap<String, String> userOptions = ImmutableMap.of();
  private ImmutableSet<String> projectSclOptions = ImmutableSet.of();
  private String configurationHash = "";
  private ImmutableSet<String> configFlags = ImmutableSet.of();

  @Override
  public void init(
      long clNumber,
      String bazelVersion,
      Collection<String> targets,
      boolean useFakeStampData,
      Map<String, String> userOptions,
      Set<String> projectSclOptions) {
    this.evaluatingVersion = clNumber;
    this.bazelVersion = bazelVersion;
    this.targets = ImmutableList.copyOf(targets);
    this.useFakeStampData = useFakeStampData;
    this.userOptions = ImmutableMap.copyOf(userOptions);
    this.projectSclOptions = ImmutableSet.copyOf(projectSclOptions);
  }

  @Override
  public void setOriginalConfigurationOptions(Set<String> configOptions) {
    ImmutableSet.Builder<String> flags = ImmutableSet.builder();
    flags.addAll(projectSclOptions);
    for (Map.Entry<String, String> entry : userOptions.entrySet()) {
      String option = entry.getKey();
      String origin = entry.getValue();
      String optionName = normalizeOptionName(option);
      if (!configOptions.contains(optionName)) {
        continue;
      }
      if (!Strings.isNullOrEmpty(origin)) {
        flags.add(origin);
      } else {
        flags.add(option);
      }
    }
    this.configFlags = flags.build();
  }

  @Override
  public void setConfigurationHash(String configurationHash) {
    this.configurationHash = configurationHash;
  }

  @Override
  public long getEvaluatingVersion() {
    return evaluatingVersion;
  }

  @Override
  public String getConfigurationHash() {
    return configurationHash;
  }

  @Override
  public String getBazelVersion() {
    return bazelVersion;
  }

  @Override
  public boolean getUseFakeStampData() {
    return useFakeStampData;
  }

  @Override
  public Collection<String> getTargets() {
    return targets;
  }

  @Override
  public Collection<String> getConfigFlags() {
    return configFlags;
  }

  private static String normalizeOptionName(String option) {
    String normalized = option.replace("'", "");
    if (normalized.startsWith("--")) {
      normalized = normalized.substring(2);
    }
    int equalsIndex = normalized.indexOf('=');
    return equalsIndex > 0 ? normalized.substring(0, equalsIndex) : normalized;
  }
}
