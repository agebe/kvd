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

import kvd.common.ByteUtils;

public abstract class TxBasePacket extends OpPacket {

  public TxBasePacket() {
    super();
  }

  public TxBasePacket(int channel) {
    super(channel);
  }

  public TxBasePacket(int channel, int txId) {
    super(channel, ByteUtils.toBytes(txId));
  }

  public TxBasePacket(int channel, byte[] body) {
    super(channel, body);
  }

  public int getTxId() {
    return getBody()!=null?ByteUtils.toInt(getBody()):-1;
  }

}
