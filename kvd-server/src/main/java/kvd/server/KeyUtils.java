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
package kvd.server;

import java.nio.charset.Charset;

import org.apache.commons.lang3.StringUtils;

import com.google.common.hash.Hashing;

import kvd.common.Utils;

public class KeyUtils {

  // only lowercase so keys that contain uppercase are hashed to support case sensitive keys
  // also on filesystems that are case insensitive
  private static final String LEGAL_CHARS = "abcdefghijklmnopqrstuvwxyz1234567890_";

  public static String internalKey(String key) {
    Utils.checkKey(key);
    if((key.length() > 200) || containsIllegalChars(key)) {
      return hashed(key);
    } else {
      return key;
    }
  }

  private static String hashed(String key) {
    return Hashing.sha256().hashString(key, Charset.forName("UTF-8")).toString();
  }

  private static boolean containsIllegalChars(String key) {
    return !StringUtils.containsOnly(key, LEGAL_CHARS);
  }

}
