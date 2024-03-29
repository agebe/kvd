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
package kvd.server.storage.fs;

import java.io.File;

import kvd.server.storage.AbortableOutputStream2;

class Staging {

  private String key;

  private File file;

  private AbortableOutputStream2<?> out;

  public Staging(String key, File file, AbortableOutputStream2<?> out) {
    this.key = key;
    this.file = file;
    this.out = out;
  }

  public String getKey() {
    return key;
  }

  public File getFile() {
    return file;
  }

  public AbortableOutputStream2<?> getOut() {
    return out;
  }

  public void abort() {
    out.abort();
  }

}
