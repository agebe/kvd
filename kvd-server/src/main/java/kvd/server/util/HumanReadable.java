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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import kvd.common.KvdException;

public class HumanReadable {

  public static Long parseDurationToMillisOrNull(String s, TimeUnit defaultTimeUnit) {
    try {
      return parseDurationToMillis(s, defaultTimeUnit);
    } catch(Throwable t) {
      return null;
    }
  }

  public static long parseDuration(String s, TimeUnit defaultTimeUnit, TimeUnit targetUnit) {
    return targetUnit.convert(parseDurationToMillis(s, defaultTimeUnit), TimeUnit.MILLISECONDS);
  }

  /**
   * Parse duration with time unit modifier and return the duration in Milliseconds.
   * Modifiers are: ms, s, m, h, d
   */
  public static long parseDurationToMillis(String s, TimeUnit defaultTimeUnit) {
    return getModifierOrMillis(s, defaultTimeUnit).toMillis(parseDigits(s));
  }

  private static long parseDigits(String s) {
    StringBuilder b = new StringBuilder();
    boolean hasDigits = false;
    for(int i=0;i<s.length();i++) {
      char c = s.charAt(i);
      if(Character.isDigit(c)) {
        b.append(c);
        hasDigits = true;
      } else if(hasDigits) {
        break;
      }
    }
    try {
      return Long.parseLong(b.toString());
    } catch(Exception e) {
      throw new KvdException("failed to parse " + s, e);
    }
  }

  private static TimeUnit getModifierOrMillis(String s, TimeUnit defaultTimeUnit) {
    if(StringUtils.endsWithIgnoreCase(s, "ms")) {
      return TimeUnit.MILLISECONDS;
    } else if(StringUtils.endsWithIgnoreCase(s, "s")) {
      return TimeUnit.SECONDS;
    } else if(StringUtils.endsWithIgnoreCase(s, "m")) {
      return TimeUnit.MINUTES;
    } else if(StringUtils.endsWithIgnoreCase(s, "h")) {
      return TimeUnit.HOURS;
    } else if(StringUtils.endsWithIgnoreCase(s, "d")) {
      return TimeUnit.DAYS;
    } else {
      return defaultTimeUnit;
    }
  }

  public static String formatDurationOrEmpty(Long duration, TimeUnit unit) {
    return duration!=null?formatDuration(duration, unit):"";
  }

  public static String formatDuration(long duration, TimeUnit unit) {
    long ms = unit.toMillis(duration);
    if((ms < 1000) && (ms > 0)) {
      return ms + " millisecond" + (duration==1?"":"s");
    } else {
      return formatDuration(ms/1000);
    }
  }

  public static String formatDuration(long seconds) {
    return seconds == 0 ? "now" :
      Arrays.stream(
          new String[]{
              formatTime("year",  (seconds / 31536000)),
              formatTime("day",   (seconds / 86400)%365),
              formatTime("hour",  (seconds / 3600)%24),
              formatTime("minute",(seconds / 60)%60),
              formatTime("second",(seconds%3600)%60)})
      .filter(e->e!="")
      .collect(Collectors.joining(", "))
      .replaceAll(", (?!.+,)", " and ");
  }

  public static String formatTime(String s, long time){
    return time==0 ? "" : Long.toString(time)+ " " + s + (time==1?"" : "s");
  }

}
