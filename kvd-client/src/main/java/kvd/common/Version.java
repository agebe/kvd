/*
 * Copyright 2021 Andre Gebers
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package kvd.common;

import java.util.jar.Attributes;
import java.util.jar.Manifest;

public class Version {

  private Attributes attributes;

  public Version(Manifest manifest) {
    this.attributes = manifest.getMainAttributes();
  }

  private String implementationVersion() {
    return attributes.getValue("Implementation-Version");
  }

  private String implementationTitle() {
    return attributes.getValue("Implementation-Title");
  }

  private String gitVersion() {
    return attributes.getValue("git-version");
  }

  public String version() {
    return String.format("%s-%s, git version %s", implementationTitle(), implementationVersion(), gitVersion());
  }

}
