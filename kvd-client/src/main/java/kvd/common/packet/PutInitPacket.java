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
package kvd.common.packet;

import kvd.common.Utils;

public class PutInitPacket extends OpPacket {

  protected PutInitPacket() {
    super();
  }

  public PutInitPacket(int channelId, String key) {
    super(channelId, Utils.toUTF8(key));
  }

  @Override
  public PacketType getType() {
    return PacketType.PUT_INIT;
  }

  public String getKey() {
    return Utils.fromUTF8(getBody());
  }

}
