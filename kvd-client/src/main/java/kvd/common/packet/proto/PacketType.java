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
package kvd.common.packet.proto;

public enum PacketType {
  HELLO,
  BYE,
  PING,
  PONG,
  PUT_INIT,
  PUT_DATA,
  PUT_FINISH,
  PUT_COMPLETE,
  GET_INIT,
  GET_DATA,
  GET_FINISH,
  CONTAINS_REQUEST,
  CONTAINS_RESPONSE,
  REMOVE_REQUEST,
  REMOVE_RESPONSE,
  CLOSE_CHANNEL,
  PUT_ABORT,
  GET_ABORT,
  CONTAINS_ABORT,
  REMOVE_ABORT,
  TX_BEGIN,
  TX_COMMIT,
  TX_ROLLBACK,
  TX_ABORT,
  TX_CLOSED,
  INVALID_REQUEST,
  LOCK,
  ABORT,
  REMOVEALL_REQUEST,
  ;
}
