package kvd.server;

import org.apache.commons.lang3.StringUtils;

public class Keys {

  public static boolean isInternalKey(String key) {
    return StringUtils.startsWith(key, "__kvd_");
  }

}
