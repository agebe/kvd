package kvd.server.storage.mem;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class TestVessel implements Serializable {

  private static final long serialVersionUID = 1379306913282591498L;

  private String name;

  private List<TestEntity> load;

  private Map<Long, List<TestEntity>> load2;

  public TestVessel(String name, List<TestEntity> load, Map<Long, List<TestEntity>> load2) {
    super();
    this.name = name;
    this.load = load;
    this.load2 = load2;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<TestEntity> getLoad() {
    return load;
  }

  public void setLoad(List<TestEntity> load) {
    this.load = load;
  }

  public Map<Long, List<TestEntity>> getLoad2() {
    return load2;
  }

  public void setLoad2(Map<Long, List<TestEntity>> load2) {
    this.load2 = load2;
  }

}
