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
package kvd.server.storage.mem;

import kvd.server.Key;
import kvd.server.storage.AbortableOutputStream2;

class Staging {

  private Key key;

  private BinaryLargeObjectOutputStream blobStream;

  private AbortableOutputStream2<?> out;

  public Staging(Key key, BinaryLargeObjectOutputStream blobStream, AbortableOutputStream2<?> out) {
    this.key = key;
    this.blobStream = blobStream;
    this.out = out;
  }

  public Key getKey() {
    return key;
  }

  public BinaryLargeObjectOutputStream getBlobStream() {
    return blobStream;
  }

  public void abort() {
    out.abort();
  }

}
