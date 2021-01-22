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
