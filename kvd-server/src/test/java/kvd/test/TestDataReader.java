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
package kvd.test;

import java.time.Duration;
import java.util.Random;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;

import kvd.client.KvdClient;
import kvd.common.KvdException;

public class TestDataReader {

  private static Duration read(int n) {
    long startNs = System.nanoTime();
    Random r = new Random(0);
    try(KvdClient client = new KvdClient("localhost")) {
      IntStream.range(0, n).mapToObj(String::valueOf).forEach(s -> {
        String v = client.getString(s);
        byte[] buf = new byte[v.length()/2];
        r.nextBytes(buf);
        if(!StringUtils.equals(TestUtils.bytesToHex(buf), v)) {
          throw new KvdException(String.format("test failed on %s/%s", s, v));
        }
      });
    }
    return Duration.ofNanos(System.nanoTime() - startNs);
  }

  public static void main(String[] args) {
    IntStream.rangeClosed(1, 100).forEachOrdered(i -> {
      int n = i * 100;
      Duration d = read(n);
      System.out.println(String.format("reading %s values took %s", n, d.toString()));
    });
    System.out.println("done");
  }
}
