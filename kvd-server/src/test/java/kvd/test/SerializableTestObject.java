package kvd.test;

import java.util.List;

public class SerializableTestObject {

  private String prop1;

  private Long prop2;

  private List<Integer> myList;

  public SerializableTestObject() {
    super();
  }

  public SerializableTestObject(String prop1, Long prop2, List<Integer> myList) {
    super();
    this.prop1 = prop1;
    this.prop2 = prop2;
    this.myList = myList;
  }

  public String getProp1() {
    return prop1;
  }

  public Long getProp2() {
    return prop2;
  }

  public List<Integer> getMyList() {
    return myList;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((myList == null) ? 0 : myList.hashCode());
    result = prime * result + ((prop1 == null) ? 0 : prop1.hashCode());
    result = prime * result + ((prop2 == null) ? 0 : prop2.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SerializableTestObject other = (SerializableTestObject) obj;
    if (myList == null) {
      if (other.myList != null)
        return false;
    } else if (!myList.equals(other.myList))
      return false;
    if (prop1 == null) {
      if (other.prop1 != null)
        return false;
    } else if (!prop1.equals(other.prop1))
      return false;
    if (prop2 == null) {
      if (other.prop2 != null)
        return false;
    } else if (!prop2.equals(other.prop2))
      return false;
    return true;
  }

}