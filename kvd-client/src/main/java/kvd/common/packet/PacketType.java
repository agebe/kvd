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

import java.util.function.Supplier;

public enum PacketType {

  HELLO(HelloPacket::new), BYE(ByePacket::new),
  PING(PingPacket::new), PONG(PongPacket::new),
  PUT_INIT(PutInitPacket::new), PUT_DATA(GenericOpPacket::new),
  PUT_FINISH(GenericOpPacket::new), PUT_COMPLETE(GenericOpPacket::new),
  GET_INIT(GenericOpPacket::new), GET_DATA(GenericOpPacket::new), GET_FINISH(GenericOpPacket::new),
  CONTAINS_REQUEST(GenericOpPacket::new), CONTAINS_RESPONSE(GenericOpPacket::new),
  REMOVE_REQUEST(GenericOpPacket::new), REMOVE_RESPONSE(GenericOpPacket::new),
  CLOSE_CHANNEL(GenericOpPacket::new),
  PUT_ABORT(GenericOpPacket::new), GET_ABORT(GenericOpPacket::new),
  CONTAINS_ABORT(GenericOpPacket::new), REMOVE_ABORT(GenericOpPacket::new),
  TX_BEGIN(TxBeginPacket::new), TX_COMMIT(TxCommitPacket::new),
  TX_ROLLBACK(TxRollbackPacket::new), TX_ABORT(TxAbortPacket::new),
  TX_CLOSED(TxClosedPacket::new)
  ;

  private final Supplier<Packet> supplier;

  private PacketType(Supplier<Packet> supplier) {
    this.supplier = supplier;
  }

  public int getId() {
    return (short)this.ordinal();
  }

  public Packet newPacket() {
    return supplier.get();
  }

  public static PacketType fromId(int id) {
    return PacketType.values()[id];
  }

  public static PacketType fromIdOrNull(int id) {
    try {
      return fromId(id);
    } catch(Exception e) {
      return null;
    }
  }

}
