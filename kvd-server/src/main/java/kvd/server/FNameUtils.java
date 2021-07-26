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
package kvd.server;

import org.apache.commons.lang3.StringUtils;

import kvd.common.KvdException;

public class FNameUtils {

  private static final String VALID_CHARS = "abcdefghijklmnopqrstuvwxyz1234567890_";

  public static String stringToFilename(String s) {
    if(StringUtils.isBlank(s)) {
      throw new KvdException("blank");
    }
    if(containsInvalidChars(s)) {
      throw new KvdException("contains invalid chars " + s);
    }
    if(s.length() > 200) {
      throw new KvdException("string length exceeds limit " + s);
    }
    return s;
  }

  private static boolean containsInvalidChars(String s) {
    return !StringUtils.containsOnly(s, VALID_CHARS);
  }
}
