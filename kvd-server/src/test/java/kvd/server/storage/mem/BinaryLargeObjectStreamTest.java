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
