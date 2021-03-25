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
package kvd.server.storage.concurrent;

public class KeyOrTx {

  private String key;

  private LockTransaction tx;

  public KeyOrTx(String key) {
    super();
    this.key = key;
  }

  public KeyOrTx(LockTransaction tx) {
    super();
    this.tx = tx;
  }

  public String getKey() {
    return key;
  }

  public LockTransaction getTx() {
    return tx;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((key == null) ? 0 : key.hashCode());
    result = prime * result + ((tx == null) ? 0 : tx.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    KeyOrTx other = (KeyOrTx) obj;
    if (key == null) {
      if (other.key != null)
        return false;
    } else if (!key.equals(other.key))
      return false;
    if (tx == null) {
      if (other.tx != null)
        return false;
    } else if (!tx.equals(other.tx))
      return false;
    return true;
  }

  @Override
  public String toString() {
    if(key != null) {
      return "key:"+key;
    } else if(tx != null) {
      return tx.toString();
    } else {
      return "KeyOrTx both null";
    }
  }

}
