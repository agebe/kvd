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

/**
 * Send from client to initiate new transaction and also from the server to confirm a new transaction has been started
 * with the tx id in the body (int)
 */
public class TxBeginPacket extends TxBasePacket {

  public TxBeginPacket() {
    super();
  }

  public TxBeginPacket(int channel, long timeoutMs) {
    super(channel);
  }

  public TxBeginPacket(int channel, int txId, long timeoutMs) {
    super(channel);
    setBody(createBody(txId, timeoutMs));
  }

  private byte[] createBody(int txId, long timeoutMs) {
    byte b[] = new byte[12];
    ByteUtils.toBytes(txId, b, 0);
    ByteUtils.toBytes(timeoutMs, b, 4);
    return b;
  }

  public long getTimeoutMs() {
    return (getBody()!=null) && (getBody().length >= 12)?ByteUtils.toLong(getBody(), 4):-1;
  }

  @Override
  public PacketType getType() {
    return PacketType.TX_BEGIN;
  }

}
