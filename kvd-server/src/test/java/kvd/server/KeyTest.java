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
package kvd.server;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class KeyTest {

  @Test
  public void test() {
    Key k1 = Key.of("k1");
    assertEquals("k1", new String(k1.getBytes()));
    Key k2 = Key.of("k2");
    assertEquals("k2", new String(k2.getBytes()));
    Key combined = Key.of(k1, k2);
    assertEquals("k1k2", new String(combined.getBytes()));
  }

}
