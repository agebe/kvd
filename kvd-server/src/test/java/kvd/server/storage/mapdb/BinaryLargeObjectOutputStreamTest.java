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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import kvd.common.KvdException;
import kvd.server.Key;
import kvd.server.util.FileUtils;
import kvd.test.TestUtils;

public class BinaryLargeObjectOutputStreamTest {

  private static final Key KEY = Key.of("k");

  private static File blobBase; 

  @BeforeEach 
  public void init() throws IOException {
    blobBase = TestUtils.createTempDirectory("blob");
  }

  @AfterEach
  public void cleanup() {
    FileUtils.deleteDirQuietly(blobBase);
  }

  @Test
  public void inline() throws IOException {
    try(BinaryLargeObjectOutputStream out = new BinaryLargeObjectOutputStream(KEY, blobBase, 6)) {
      byte[] b = new byte[] {1,2,3,4,5,6,7,8,9, 10};
      out.write(b, 0, 2);
      out.write(b, 2, 3);
      out.write(20);
      out.close();
      Value v = out.toValue();
      assertEquals(ValueType.INLINE, v.getType());
      assertArrayEquals(new byte[] {1,2,3,4,5, 20}, v.inline());
      assertEquals(0, blobBase.list().length);
    }
  }

  @Test
  public void blob() throws IOException {
    try(BinaryLargeObjectOutputStream out = new BinaryLargeObjectOutputStream(KEY, blobBase, 5)) {
      byte[] b = new byte[] {1,2,3,4,5,6,7,8,9, 10};
      out.write(b);
      out.close();
      Value v = out.toValue();
      assertEquals(ValueType.BLOB, v.getType());
      assertEquals(1, blobBase.list().length);
      assertEquals(blobBase.list()[0], v.blobs().get(0));
      assertArrayEquals(b, getContentFromBlob(new File(blobBase, v.blobs().get(0))));
      byte[] buf = v.serialize();
      Value v2 = Value.deserialize(buf);
      assertEquals(ValueType.BLOB, v2.getType());
      assertEquals(blobBase.list()[0], v2.blobs().get(0));
      assertArrayEquals(b, getContentFromBlob(new File(blobBase, v2.blobs().get(0))));
    }
  }

  private byte[] getContentFromBlob(File blob) throws IOException {
    try(InputStream in = new FileInputStream(blob)) {
      BlobHeader.fromInputStream(in);
      return in.readAllBytes();
    }
  }

  @Test
  public void blobSplit() throws IOException {
    try(BinaryLargeObjectOutputStream out = new BinaryLargeObjectOutputStream(
        KEY, blobBase, 0, BlobHeader.headerLength(KEY)+3)) {
      byte[] b = new byte[] {1,2,3,4,5,6,7,8,9, 10};
      out.write(b);
      out.close();
      Value v = out.toValue();
      assertEquals(ValueType.BLOB, v.getType());
      assertEquals(4, blobBase.list().length);
      Set<String> files = Arrays.stream(blobBase.list()).collect(Collectors.toSet());
      assertTrue(files.contains(blobBase.list()[0]));
      assertTrue(files.contains(blobBase.list()[1]));
      assertTrue(files.contains(blobBase.list()[2]));
      assertTrue(files.contains(blobBase.list()[3]));
      assertArrayEquals(new byte[] {1,2,3}, getContentFromBlob(new File(blobBase, v.blobs().get(0))));
      assertArrayEquals(new byte[] {4,5,6}, getContentFromBlob(new File(blobBase, v.blobs().get(1))));
      assertArrayEquals(new byte[] {7,8,9}, getContentFromBlob(new File(blobBase, v.blobs().get(2))));
      assertArrayEquals(new byte[] {10}, getContentFromBlob(new File(blobBase, v.blobs().get(3))));
      byte[] buf = v.serialize();
      Value v2 = Value.deserialize(buf);
      assertEquals(4, v2.blobs().size());
      assertEquals(4, new HashSet<>(v2.blobs()).size());
      assertTrue(files.contains(v2.blobs().get(0)));
      assertTrue(files.contains(v2.blobs().get(1)));
      assertTrue(files.contains(v2.blobs().get(2)));
      assertTrue(files.contains(v2.blobs().get(3)));
    }
  }

  @Test
  public void notClosed() throws IOException {
    try(BinaryLargeObjectOutputStream out = new BinaryLargeObjectOutputStream(KEY, blobBase)) {
      assertThrows(KvdException.class, () -> out.toValue());
    }
  }

}
