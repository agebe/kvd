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
package kvd.server.storage.mapdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

public class ValueTest {

  @Test
  public void testInline() {
    byte[] inline = new byte[] {-2,4,111,127,-128};
    Value v1 = Value.inline(inline);
    Instant c = v1.getCreated();
    Instant a = v1.getAccessed();
    byte[] s = v1.serialize();
    Value v2 = Value.deserialize(s);
    assertArrayEquals(inline, v2.inline());
    assertEquals(c, v2.getCreated());
    assertEquals(a, v2.getAccessed());
  }

  @Test
  public void testBlob() {
    var l = List.of("1", "2", UUID.randomUUID().toString());
    Value v1 = Value.blob(l);
    Instant c = v1.getCreated();
    Instant a = v1.getAccessed();
    byte[] s = v1.serialize();
    Value v2 = Value.deserialize(s);
    assertEquals(l, v2.blobs());
    assertEquals(c, v2.getCreated());
    assertEquals(a, v2.getAccessed());
  }


}

