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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

public class HumanReadableTest {

  @Test
  public void test() {
    assertEquals(10000, HumanReadable.parseDurationToMillis("10s", TimeUnit.SECONDS));
    assertEquals(10, HumanReadable.parseDuration("10", TimeUnit.SECONDS, TimeUnit.SECONDS));
    assertEquals(60, HumanReadable.parseDuration("1", TimeUnit.HOURS, TimeUnit.MINUTES));
    assertEquals(60, HumanReadable.parseDuration("1h", TimeUnit.SECONDS, TimeUnit.MINUTES));
  }

}
