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

import java.util.Arrays;

public class HelloPacket extends Packet {

  private static final byte[] HELLO = new byte[] {'K', 'v', 'd','H','e','l','l','o', '1'};

  public HelloPacket() {
    super(HELLO);
  }

  public PacketType getType() {
    return PacketType.HELLO;
  }

  public static boolean isHello(Packet p) {
    return (p instanceof HelloPacket) && Arrays.equals(HELLO, p.getBody());
  }

}
