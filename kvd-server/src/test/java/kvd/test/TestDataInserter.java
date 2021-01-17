package kvd.test;

import java.util.Random;
import java.util.stream.IntStream;

import kvd.client.KvdClient;

public class TestDataInserter {

  // https://unix.stackexchange.com/questions/197633/how-to-use-the-new-ext4-inline-data-feature-storing-data-directly-in-the-inod/197806#197806
  public static void main(String[] args) {
    int inserts = 1_000_000;
    Random r = new Random(0);
    byte[] buf = new byte[64];
    try(KvdClient client = new KvdClient("localhost")) {
      IntStream.range(0, inserts).forEachOrdered(i -> {
        r.nextBytes(buf);
        client.putString("128_"+String.valueOf(i), TestUtils.bytesToHex(buf));
        if((i % 1000 == 0) && (i > 0)) {
          System.out.println(String.format("done inserting '%s' key/value pairs", i));
        }
      });
    }
    System.out.println(String.format("done inserting '%s' key/value pairs", inserts));
    System.out.println("done");
  }

}
