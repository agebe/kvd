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

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import kvd.common.Utils;
import kvd.server.Key;
import kvd.server.list.KvdLinkedList;
import kvd.server.storage.StorageBackend;
import kvd.server.storage.mem.MemStorageBackend;

public class LookupTest {

  public static void main(String[] args) throws Exception {
    StorageBackend storage;
//    Path tempDirWithPrefix = Files.createTempDirectory("kvd");
//    storage = new FileStorageBackend(tempDirWithPrefix.toFile(), new SimpleTrash());
    storage = new MemStorageBackend();
    KvdLinkedList<String> l = new KvdLinkedList<>(storage,
        "lookup2",
        Utils::toUTF8,
        Utils::fromUTF8,
        s -> Key.of(s));
    IntStream.range(0, 5_000).forEach(i -> l.add(Integer.toString(i)));
    {
      System.out.println("start lookups");
      long start = System.nanoTime();
      IntStream.range(0, 1_000).forEach(i -> l.lookup(Key.of("2500")));
      long duration = System.nanoTime() - start;
      System.out.println(String.format("1000 lookups took %sms", TimeUnit.NANOSECONDS.toMillis(duration)));
    }
    {
      System.out.println("start get");
      long start = System.nanoTime();
      IntStream.range(0, 100).forEach(i -> l.get(2500));
      long duration = System.nanoTime() - start;
      System.out.println(String.format("100 gets took %sms", TimeUnit.NANOSECONDS.toMillis(duration)));
    }
    System.out.println("done");
  }

}
