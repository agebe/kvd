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
package kvd.server.storage.mapdb;

public class Value {

  private ValueType type;

  // if type is INLINE byte array contains the value
  // if type is BLOB byte array contains a reference to the blob store
  // if type is REMOVE byte array is null
  private byte[] value;

  public Value(ValueType type, byte[] value) {
    super();
    this.type = type;
    this.value = value;
  }

  public ValueType getType() {
    return type;
  }

  public byte[] getValue() {
    return value;
  }

}