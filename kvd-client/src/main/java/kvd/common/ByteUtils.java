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
package kvd.common;

public class ByteUtils {

  public static long toLong(byte[] b, int offset) {
    return ((long) b[7+offset] << 56)
        | ((long) b[6+offset] & 0xff) << 48
        | ((long) b[5+offset] & 0xff) << 40
        | ((long) b[4+offset] & 0xff) << 32
        | ((long) b[3+offset] & 0xff) << 24
        | ((long) b[2+offset] & 0xff) << 16
        | ((long) b[1+offset] & 0xff) << 8
        | ((long) b[offset] & 0xff);
  }

  public static long toLong(byte[] b) {
    return toLong(b, 0);
  }

  public static int toInt(byte[] b, int offset) {
    return (b[3+offset] & 0xff) << 24
        | (b[2+offset] & 0xff) << 16
        | (b[1+offset] & 0xff) << 8
        | (b[offset] & 0xff);
  }

  public static int toInt(byte[] b) {
    return toInt(b, 0);
  }

  public static byte[] toBytes(long l, byte[] b, int offset) {
    b[0+offset] = (byte)l;
    b[1+offset] = (byte) (l >>> 8);
    b[2+offset] = (byte) (l >>> 16);
    b[3+offset] = (byte) (l >>> 24);
    b[4+offset] = (byte) (l >>> 32);
    b[5+offset] = (byte) (l >>> 40);
    b[6+offset] = (byte) (l >>> 48);
    b[7+offset] = (byte) (l >>> 56);
    return b;
  }

  public static byte[] toBytes(long l) {
    return toBytes(l, new byte[8], 0);
  }

  public static byte[] toBytes(int i, byte[] b, int offset) {
    b[0+offset] = (byte)i;
    b[1+offset] = (byte) (i >>> 8);
    b[2+offset] = (byte) (i >>> 16);
    b[3+offset] = (byte) (i >>> 24);
    return b;
  }

  public static byte[] toBytes(int i) {
    return toBytes(i, new byte[4], 0);
  }

}
