package kvd.server.storage.mem;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class BinaryLargeObject {

  private int blockSize;

  List<byte[]> blocks;

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
    Objects.checkFromIndexSize(off, len, buf.length);
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
    Objects.checkFromIndexSize(off, len, buf.length);
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

  public long size() {
    return size;
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  public void compact() {
    // TODO compact internal data structures. e.g. write all into single byte array if it fits or find optimal 
    // block size and reorganize
    // meant to be called after all writing to the blob has finished
  }

}
