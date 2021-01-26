package kvd.common;

public class IOStreamUtils {

  /**
   * java 8 version of java 9 {@code java.util.Objects::checkFromIndexSize}
   */
  private static void checkFromIndexSize(int off, int len, int arraylength) {
    if ((off | len | (arraylength - (len + off)) | (off + len)) < 0)
      throw new IndexOutOfBoundsException();
  }

  public static void checkFromIndexSize(byte[] b, int off, int len) {
    checkFromIndexSize(off, len, b.length);
  }

}
