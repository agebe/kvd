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
package kvd.server.storage.mem;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.junit.jupiter.api.Test;

import com.google.gson.Gson;

public class BinaryLargeObjectStreamTest {

  @Test
  public void oosTest() throws Exception {
    BinaryLargeObject blob = null;
    int entities = 1_000;
    {
      List<TestEntity> l = LongStream.range(0, entities)
          .mapToObj(i -> new TestEntity(i, 1024)).collect(Collectors.toList());
      TestVessel vessel = new TestVessel("test1", l, l.stream().collect(Collectors.groupingBy(t -> t.getId())));
      BinaryLargeObjectOutputStream blobStream = new BinaryLargeObjectOutputStream();
      try(ObjectOutputStream out = new ObjectOutputStream(blobStream)) {
        out.writeObject(vessel);
      }
      blob = blobStream.toBinaryLargeObject();
      assertFalse(blob.isEmpty());
    }
    {
      try(ObjectInputStream in = new ObjectInputStream(new BinaryLargeObjectInputStream(blob))) {
        TestVessel vessel = (TestVessel)in.readObject();
        assertEquals(entities, vessel.getLoad().size());
        assertEquals(entities-1, vessel.getLoad().get(entities-1).getId());
        assertEquals("test1", vessel.getName());
        assertEquals(entities, vessel.getLoad2().size());
      }
    }
  }

  @Test
  public void gsonTest() throws Exception {
    BinaryLargeObject blob = null;
    int entities = 100;
    {
      List<TestEntity> l = LongStream.range(0, entities)
          .mapToObj(i -> new TestEntity(i, 1024)).collect(Collectors.toList());
      TestVessel vessel = new TestVessel("test1", l, l.stream().collect(Collectors.groupingBy(t -> t.getId())));
      BinaryLargeObjectOutputStream blobStream = new BinaryLargeObjectOutputStream();
      try(OutputStreamWriter writer = new OutputStreamWriter(blobStream)) {
        new Gson().toJson(vessel, writer);
      }
      blob = blobStream.toBinaryLargeObject();
      assertFalse(blob.isEmpty());
    }
    {
      try(InputStreamReader reader = new InputStreamReader(new BinaryLargeObjectInputStream(blob))) {
        TestVessel vessel = new Gson().fromJson(reader, TestVessel.class);
        assertEquals(entities, vessel.getLoad().size());
        assertEquals(entities-1, vessel.getLoad().get(entities-1).getId());
        assertEquals("test1", vessel.getName());
        assertEquals(entities, vessel.getLoad2().size());
      }
    }
  }

}
