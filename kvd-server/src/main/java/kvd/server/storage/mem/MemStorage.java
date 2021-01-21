package kvd.server.storage.mem;

import java.io.InputStream;
import java.io.OutputStream;

import kvd.server.storage.StorageBackend;

public class MemStorage implements StorageBackend {

  @Override
  public OutputStream begin(String key) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void commit(String key) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void rollack(String key) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public InputStream get(String key) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean contains(String key) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean remove(String key) {
    // TODO Auto-generated method stub
    return false;
  }

}
