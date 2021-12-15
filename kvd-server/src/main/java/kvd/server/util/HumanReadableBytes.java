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
package kvd.server.util;

import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.TEN;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import kvd.common.KvdException;

public class HumanReadableBytes {

  private static final BigDecimal KIBI = BigDecimal.valueOf(1024);

  public static long parseLong(String s) {
    return parse(s).longValueExact();
  }

  public static int parseInt(String s) {
    return parse(s).intValueExact();
  }

  public static BigInteger parse(String s) {
    Matcher m = Pattern.compile("([\\d.,]+)\\s*(\\w*)").matcher(StringUtils.strip(s));
    if(m.matches()) {
      return new BigDecimal(m.group(1)).multiply(scale(StringUtils.lowerCase(m.group(2)))).toBigInteger();
    } else {
      throw new KvdException("failed to parse size: " + s);
    }
  }

  private static BigDecimal scale(String unit) {
    if(StringUtils.isBlank(unit)) {
      return ONE;
    }
    switch(unit) {
    case "b" : return ONE;

    case "k" : return TEN.pow(3);
    case "kb": return TEN.pow(3);
    case "ki": return KIBI;
    case "kib": return KIBI;
    case "kibi": return KIBI;

    case "m" : return TEN.pow(6);
    case "mb": return TEN.pow(6);
    case "mi": return KIBI.pow(2);
    case "mib": return KIBI.pow(2);
    case "mebi": return KIBI.pow(2);

    case "g" : return TEN.pow(9);
    case "gb": return TEN.pow(9);
    case "gi": return KIBI.pow(3);
    case "gib": return KIBI.pow(3);
    case "gibi": return KIBI.pow(3);

    case "t" : return TEN.pow(12);
    case "tb": return TEN.pow(12);
    case "ti": return KIBI.pow(4);
    case "tib": return KIBI.pow(4);
    case "tebi": return KIBI.pow(4);

    default: throw new KvdException("unit not recognized: " + unit);
    }
  }

  // from https://stackoverflow.com/a/3758880
  public static String formatSI(long bytes) {
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

  // from https://stackoverflow.com/a/3758880
  public static String formatBin(long bytes) {
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
