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
package kvd.server.util;

import static kvd.server.util.HumanReadableBytes.parseInt;
import static kvd.server.util.HumanReadableBytes.parseLong;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class HumanReadableBytesTest {

  @Test
  public void test() {
    assertEquals(0, parseLong("0"));
    assertEquals(1, parseLong("1"));
    assertEquals(1, parseLong("1 "));
    assertEquals(1, parseLong("1 b"));
    assertEquals(1, parseLong("1b"));
    assertEquals(1000, parseLong("1kb"));
    assertEquals(2000, parseLong("2kb"));
    assertEquals(2500, parseLong("2.5kb"));
    assertEquals(1000, parseLong("1k"));
    assertEquals(2048, parseLong("2kib"));
    assertEquals(65536, parseInt("64ki"));
    assertEquals(5_000_000, parseLong("5mb"));
    assertEquals(1024*1024, parseLong("1mib"));
    assertEquals(1024*1024*1024, parseLong("1gi"));
    assertEquals(2l * 1024*1024*1024, parseLong("2gi"));
    assertThrows(ArithmeticException.class, () -> parseInt("2gi"));
    assertEquals(5l*1024l*1024l*1024l, parseLong("5gi"));
    assertThrows(ArithmeticException.class, () -> parseInt("5g"));
  }
}
