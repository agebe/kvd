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
