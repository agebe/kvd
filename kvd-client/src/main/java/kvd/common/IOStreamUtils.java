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
