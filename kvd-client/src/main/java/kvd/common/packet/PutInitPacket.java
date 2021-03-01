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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import kvd.common.Utils;

public class PutInitPacket extends OpPacket {

  private int txId;

  protected PutInitPacket() {
    super();
  }

  public PutInitPacket(int channelId, int txId, String key) {
    super(channelId, Utils.toUTF8(key));
    this.txId = txId;
  }

  @Override
  public PacketType getType() {
    return PacketType.PUT_INIT;
  }

  @Override
  protected void readHeaders(DataInputStream in) throws IOException {
    super.readHeaders(in);
    txId = in.readInt();
  }

  @Override
  protected void writeHeader(DataOutputStream out) throws IOException {
    super.writeHeader(out);
    out.writeInt(txId);
  }

  public String getKey() {
    return Utils.fromUTF8(getBody());
  }

  public int getTxId() {
    return txId;
  }

}
