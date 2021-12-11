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
package kvd.server.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import kvd.common.KvdException;
import kvd.common.Utils;
import kvd.server.Key;

public interface Transaction extends AutoCloseable {

  AbortableOutputStream put(Key key);

  InputStream get(Key key);

  boolean contains(Key key);

  boolean remove(Key key);

  boolean lock(Key key);

  // get a write lock on the key without waiting or throw exception
  // also succeeds if locks are not supported (concurrency NONE)
  default void writeLockNowOrFail(Key key) {}

  void removeAll();

  default void putBytes(Key key, byte[] bytes) {
    try(OutputStream out = put(key)) {
      out.write(bytes);
    } catch(IOException e) {
      throw new KvdException("put bytes failed", e);
    }
  }

  default byte[] getBytes(Key key) {
    try(InputStream in = get(key)) {
      if(in != null) {
        return Utils.toByteArray(in);
      } else {
        return null;
      }
    } catch(IOException e) {
      throw new KvdException("get bytes failed", e);
    }
  }

  /**
   * commit and close.
   */
  void commit();

  /**
   * Undo changes made by the transaction so far. The transaction is also closed and
   * can not be used anymore.
   */
  void rollback();

  default void close() {
    rollback();
  }

  int handle();

}
