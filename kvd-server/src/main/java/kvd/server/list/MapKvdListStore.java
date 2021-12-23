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
package kvd.server.list;

import java.util.Map;

import kvd.server.Key;

public class MapKvdListStore implements KvdListStore {

  private Map<Key, byte[]> m;

  public MapKvdListStore(Map<Key, byte[]> m) {
    super();
    this.m = m;
  }

  @Override
  public byte[] get(Key key) {
    return m.get(key);
  }

  @Override
  public void put(Key key, byte[] value) {
    m.put(key, value);
  }

  @Override
  public void remove(Key key) {
    m.remove(key);
  }

}
