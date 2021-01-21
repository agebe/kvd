package kvd.server.storage.mem;

import java.io.Serializable;

public class TestEntity implements Serializable {

  private static final long serialVersionUID = -2303172487362932149L;

  private long id;

  private long[] ballast;

  public TestEntity(long id, int ballastSize) {
    super();
    this.id = id;
    ballast = new long[ballastSize];
  }

  public long getId() {
    return id;
  }

  public long[] getBallast() {
    return ballast;
  }

  public void setBallast(long[] ballast) {
    this.ballast = ballast;
  }

}
