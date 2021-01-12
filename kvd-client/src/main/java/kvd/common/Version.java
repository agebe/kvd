package kvd.common;

import java.util.jar.Attributes;
import java.util.jar.Manifest;

public class Version {

  private Attributes attributes;

  public Version(Manifest manifest) {
    this.attributes = manifest.getMainAttributes();
  }

  private String implementationVersion() {
    return attributes.getValue("Implementation-Version");
  }

  private String implementationTitle() {
    return attributes.getValue("Implementation-Title");
  }

  private String gitVersion() {
    return attributes.getValue("git-version");
  }

  public String version() {
    return String.format("%s-%s, git version %s", implementationTitle(), implementationVersion(), gitVersion());
  }

}
