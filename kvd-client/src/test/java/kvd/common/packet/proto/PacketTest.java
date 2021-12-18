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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import kvd.common.Utils;

public class PacketTest {

  @Test
  public void test1() throws Exception {
//    Arrays.stream(ManagementFactory.getRuntimeMXBean().getClassPath().split(":")).forEach(System.out::println);
    Packet hello = Packet.newBuilder()
        .setType(PacketType.HELLO)
        .setByteBody(ByteString.copyFrom(Utils.toUTF8("KvdHello1")))
        .build();
    Packet ping = Packet.newBuilder()
        .setType(PacketType.PING)
        .build();
    Packet putInit = Packet.newBuilder()
        .setType(PacketType.PUT_INIT)
        .setChannel(1)
        .setTx(5)
        .setPutInit(PutInitBody.newBuilder()
            .setTtlMs(TimeUnit.HOURS.toMillis(1))
            .setKey(ByteString.copyFromUtf8("testkey"))
            .build())
        .build();
    Packet bye = Packet.newBuilder()
        .setType(PacketType.BYE)
        .build();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    hello.writeDelimitedTo(out);
    ping.writeDelimitedTo(out);
    putInit.writeDelimitedTo(out);
    bye.writeDelimitedTo(out);
    byte[] buf = out.toByteArray();
    //System.out.println("byte array size: " + buf.length);
    ByteArrayInputStream in = new ByteArrayInputStream(buf);
    Packet p1 = Packet.parseDelimitedFrom(in);
    Packet p2 = Packet.parseDelimitedFrom(in);
    Packet p3 = Packet.parseDelimitedFrom(in);
    Packet p4 = Packet.parseDelimitedFrom(in);
    Packet p5 = Packet.parseDelimitedFrom(in);
    assertEquals(hello, p1);
    assertEquals(ping, p2);
    assertEquals(putInit, p3);
    assertEquals(1, p3.getChannel());
    assertEquals(5, p3.getTx());
    assertEquals(TimeUnit.HOURS.toMillis(1), p3.getPutInit().getTtlMs());
    assertEquals("testkey", p3.getPutInit().getKey().toStringUtf8());
    assertEquals(bye, p4);
    assertNull(p5);
  }

}
