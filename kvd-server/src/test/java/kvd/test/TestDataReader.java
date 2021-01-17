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
