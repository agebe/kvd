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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import kvd.server.util.FileUtils;
import kvd.test.TestUtils;

public class BinaryLargeObjectInputStreamTest {
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
    byte[] b = new byte[] {1,2,3,4,5,6,7,8,9, 10};
    Value v;
    try(BinaryLargeObjectOutputStream out = new BinaryLargeObjectOutputStream(blobBase)) {
      out.write(b);;
      out.close();
      v = out.toValue();
      assertEquals(ValueType.INLINE, v.getType());
    }
    try(InputStream in = new BinaryLargeObjectInputStream(blobBase, v)) {
      assertArrayEquals(b, in.readAllBytes());
    }
  }

  @Test
  public void blob() throws IOException {
    byte[] b = new byte[] {1,2,3,4,5,6,7,8,9, 10};
    Value v;
    try(BinaryLargeObjectOutputStream out = new BinaryLargeObjectOutputStream(blobBase, 3)) {
      out.write(b);
      out.close();
      v = out.toValue();
      assertEquals(ValueType.BLOB, v.getType());
    }
    try(InputStream in = new BinaryLargeObjectInputStream(blobBase, v)) {
      assertArrayEquals(b, in.readAllBytes());
    }
  }

  @Test
  public void blobSplit() throws IOException {
    byte[] b = new byte[] {1,2,3,4,5,6,7,8,9, 10};
    Value v;
    try(BinaryLargeObjectOutputStream out = new BinaryLargeObjectOutputStream(blobBase, 0, 3)) {
      out.write(b);
      out.close();
      v = out.toValue();
      assertEquals(ValueType.BLOB, v.getType());
    }
    try(InputStream in = new BinaryLargeObjectInputStream(blobBase, v)) {
      assertArrayEquals(b, in.readAllBytes());
    }
  }
}
