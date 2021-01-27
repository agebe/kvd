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

import java.util.ArrayList;
import java.util.List;

import kvd.common.IOStreamUtils;

public class BinaryLargeObject {

  // TODO arbitrary threshold, should be passed in rather than hard-coded.
  private static final double WASTE_RATIO_THRESHOLD = 0.001;

  private int blockSize;

  private List<byte[]> blocks;

  private long size;

  public BinaryLargeObject() {
    this(1024);
  }

  public BinaryLargeObject(int blockSize) {
    if(blockSize <= 0) {
      throw new IllegalArgumentException("block size must be > 0");
    }
    this.blockSize = blockSize;
    this.blocks = new ArrayList<>();
  }

  public void append(byte[] buf, int off, int len) {
    IOStreamUtils.checkFromIndexSize(buf, off, len);
    int copied = 0;
    while(copied < len) {
      long blockIndex = size / blockSize;
      // TODO this probably blows up before we reach Integer.MAX_VALUE, should we care?
      if(blockIndex > Integer.MAX_VALUE) {
        throw new RuntimeException(String.format("no space left in binary large object"));
      }
      int i = (int)blockIndex;
      if(i >= blocks.size()) {
        blocks.add(new byte[blockSize]);
      }
      byte[] block = blocks.get(i);
      int blockStart = (int)(size % blockSize);
      int copyLen = Math.min(blockSize-blockStart, len-copied);
      System.arraycopy(buf, copied+off, block, blockStart, copyLen);
      copied += copyLen;
      size += copyLen;
    }
  }

  public void append(byte[] buf) {
    append(buf, 0, buf.length);
  }

  public void read(long index, byte[] buf, int off, int len) {
    IOStreamUtils.checkFromIndexSize(buf, off, len);
    if((index < 0) || ((index+len) > size)) {
      throw new IndexOutOfBoundsException(Long.toString(index+len));
    }
    int copied = 0;
    while(copied < len) {
      long blockIndex = index / blockSize;
      int i = (int)blockIndex;
      byte[] block = blocks.get(i);
      int blockStart = (int)(index % blockSize);
      int copyLen = Math.min(blockSize-blockStart, len-copied);
      System.arraycopy(block, blockStart, buf, copied+off, copyLen);
      copied += copyLen;
      index += copyLen;
    }
  }

  public void read(long index, byte[] buf) {
    read(index, buf, 0, buf.length);
  }

  /**
   * @return number of bytes written into this {@code BinaryLargeObject}
   */
  public long size() {
    return size;
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  /**
   * @return number of blocks multiplied by block size
   */
  public long byteSize() {
    return blocks.size() * blockSize;
  }

  // amount of wasted bytes (partially filled last block)
  private int waste() {
    int blockStart = (int)(size % blockSize);
    return blockStart==0?0:blockSize-blockStart;
  }

  /**
   * Compact internal data structures.
   */
  public void compact() {
    if(isEmpty()) {
      return;
    }
    // TODO simple implementation meant to shrink to large blocks into a single block of exact size
    // to reduce wasted memory to 0 but only do it if we are above the threshold.
    // More could be done here i think but good enough for now.
    // Meant to be called after all writing to the blob has finished.
    int waste = waste();
    double wasteRatio = (double)waste/(double)size;
//    System.out.println(String.format("size '%s', block-size '%s', waste '%s', ratio '%.6f'",
//        size, blockSize, waste, wasteRatio));
    if(wasteRatio > WASTE_RATIO_THRESHOLD) {
      if(size < (Integer.MAX_VALUE - 1024)) {
        byte[] newBuf = new byte[(int)size];
        for(int i=0;i<blocks.size();i++) {
          System.arraycopy(blocks.get(i), 0, newBuf, i*blockSize, (int)Math.min(blockSize, size-i*blockSize));
        }
        blocks.clear();
        blocks.add(newBuf);
        blockSize = newBuf.length;
//        int wasteAfter = waste();
//        double wasteRatioAfter = (double)wasteAfter/(double)size;
//        System.out.println(String.format("after compact, size '%s', block-size '%s', waste '%s', ratio '%.6f'",
//            size, blockSize, wasteAfter, wasteRatioAfter));
      } else {
        // TODO not sure if this case is relevant
      }
    }
  }

}
