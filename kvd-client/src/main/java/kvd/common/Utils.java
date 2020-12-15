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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.net.Socket;
import java.time.Duration;

public class Utils {

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

  public static void receiveHello(DataInputStream in) {
    long lastReceiveNs = System.nanoTime();
    while(true) {
      Packet p;
      try {
        p = Packet.readNext(in);
      } catch (EOFException e) {
        throw new KvdException("eof on hello");
      }
      if(p != null) {
        if(!HelloPacket.isHello(p)) {
          throw new KvdException("hello mismatch");
        } else {
          break;
        }
      }
      if(isTimeout(lastReceiveNs, 10)) {
        throw new KvdException("timeout waiting for hello packet");
      }
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

}
