/*
 * Copyright 2020 Andre Gebers
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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.Charset;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.time.Duration;

public class Utils {

  public static final int EOF = -1;

  public static void closeSocketQuietly(Socket socket) {
    closeQuietly(socket);
  }

  public static void closeQuietly(AutoCloseable c) {
    try {
      c.close();
    } catch(Exception e) {
      // ignore
    }
  }

  public static boolean isTimeout(long startNs, long timeoutSeconds) {
    return Duration.ofNanos(System.nanoTime()-startNs).getSeconds() >= timeoutSeconds;
  }

  public static void checkTimeout(long startNs, long timeoutSeconds) {
    if(isTimeout(startNs, timeoutSeconds)) {
      throw new KvdException("timeout");
    }
  }

  public static byte[] toUTF8(String s) {
    try {
      return s.getBytes("UTF-8");
    } catch(Exception e) {
      throw new KvdException("failed to utf8 encode string", e);
    }
  }

  public static String fromUTF8(byte[] buf) {
    try {
      return new String(buf, "UTF-8");
    } catch(Exception e) {
      throw new KvdException("failed to utf8 decode to string", e);
    }
  }

  // copied from apache commons lang3
  /*
   * Licensed to the Apache Software Foundation (ASF) under one or more
   * contributor license agreements.  See the NOTICE file distributed with
   * this work for additional information regarding copyright ownership.
   * The ASF licenses this file to You under the Apache License, Version 2.0
   * (the "License"); you may not use this file except in compliance with
   * the License.  You may obtain a copy of the License at
   *
   *      http://www.apache.org/licenses/LICENSE-2.0
   *
   * Unless required by applicable law or agreed to in writing, software
   * distributed under the License is distributed on an "AS IS" BASIS,
   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   * See the License for the specific language governing permissions and
   * limitations under the License.
   */
  public static boolean isBlank(final CharSequence cs) {
    final int strLen = length(cs);
    if (strLen == 0) {
      return true;
    }
    for (int i = 0; i < strLen; i++) {
      if (!Character.isWhitespace(cs.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  // copied from apache commons lang3
  /*
   * Licensed to the Apache Software Foundation (ASF) under one or more
   * contributor license agreements.  See the NOTICE file distributed with
   * this work for additional information regarding copyright ownership.
   * The ASF licenses this file to You under the Apache License, Version 2.0
   * (the "License"); you may not use this file except in compliance with
   * the License.  You may obtain a copy of the License at
   *
   *      http://www.apache.org/licenses/LICENSE-2.0
   *
   * Unless required by applicable law or agreed to in writing, software
   * distributed under the License is distributed on an "AS IS" BASIS,
   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   * See the License for the specific language governing permissions and
   * limitations under the License.
   */
  public static int length(final CharSequence cs) {
    return cs == null ? 0 : cs.length();
  }

  public static void checkKey(String key) {
    if(isBlank(key)) {
      throw new KvdException("invalid key (blank)");
    }
  }

  public static File getUserHome() {
    return new File(System.getProperty("user.home"));
  }

  //copied from apache commons io
  /*
   * Licensed to the Apache Software Foundation (ASF) under one or more
   * contributor license agreements.  See the NOTICE file distributed with
   * this work for additional information regarding copyright ownership.
   * The ASF licenses this file to You under the Apache License, Version 2.0
   * (the "License"); you may not use this file except in compliance with
   * the License.  You may obtain a copy of the License at
   *
   *      http://www.apache.org/licenses/LICENSE-2.0
   *
   * Unless required by applicable law or agreed to in writing, software
   * distributed under the License is distributed on an "AS IS" BASIS,
   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   * See the License for the specific language governing permissions and
   * limitations under the License.
   */
  /**
   * Returns a Charset for the named charset. If the name is null, return the default Charset.
   *
   * @param charsetName
   *            The name of the requested charset, may be null.
   * @return a Charset for the named charset
   * @throws java.nio.charset.UnsupportedCharsetException
   *             If the named charset is unavailable
   */
  public static Charset toCharset(final String charsetName) {
      return charsetName == null ? Charset.defaultCharset() : Charset.forName(charsetName);
  }

  //copied from apache commons io
  /*
   * Licensed to the Apache Software Foundation (ASF) under one or more
   * contributor license agreements.  See the NOTICE file distributed with
   * this work for additional information regarding copyright ownership.
   * The ASF licenses this file to You under the Apache License, Version 2.0
   * (the "License"); you may not use this file except in compliance with
   * the License.  You may obtain a copy of the License at
   *
   *      http://www.apache.org/licenses/LICENSE-2.0
   *
   * Unless required by applicable law or agreed to in writing, software
   * distributed under the License is distributed on an "AS IS" BASIS,
   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   * See the License for the specific language governing permissions and
   * limitations under the License.
   */
  /**
   * Copies bytes from a large (over 2GB) <code>InputStream</code> to an
   * <code>OutputStream</code>.
   * <p>
   * This method uses the provided buffer, so there is no need to use a
   * <code>BufferedInputStream</code>.
   * </p>
   *
   * @param input the <code>InputStream</code> to read from
   * @param output the <code>OutputStream</code> to write to
   * @param buffer the buffer to use for the copy
   * @return the number of bytes copied. or {@code 0} if {@code input is null}.
   * @throws IOException if an I/O error occurs
   * @since 2.2
   */
  public static long copyLarge(final InputStream input, final OutputStream output, final byte[] buffer)
      throws IOException {
      long count = 0;
      if (input != null) {
          int n;
          while (EOF != (n = input.read(buffer))) {
              output.write(buffer, 0, n);
              count += n;
          }
      }
      return count;
  }

  public static byte[] toByteArray(final InputStream input) throws IOException {
    try (final ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      copyLarge(input, output, new byte[4096]);
      return output.toByteArray();
    }
  }

  // from https://stackoverflow.com/a/3758880
  public static String humanReadableByteCountSI(long bytes) {
    if (-1000 < bytes && bytes < 1000) {
      return bytes + " B";
    }
    CharacterIterator ci = new StringCharacterIterator("kMGTPE");
    while (bytes <= -999_950 || bytes >= 999_950) {
      bytes /= 1000;
      ci.next();
    }
    return String.format("%.1f %cB", bytes / 1000.0, ci.current());
  }

  public static String humanReadableByteCountBin(long bytes) {
    long absB = bytes == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(bytes);
    if (absB < 1024) {
      return bytes + " B";
    }
    long value = absB;
    CharacterIterator ci = new StringCharacterIterator("KMGTPE");
    for (int i = 40; i >= 0 && absB > 0xfffccccccccccccL >> i; i -= 10) {
      value >>= 10;
      ci.next();
    }
    value *= Long.signum(bytes);
    return String.format("%.1f %ciB", value / 1024.0, ci.current());
  }

}
