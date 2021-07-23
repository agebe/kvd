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
package kvd.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Future;

import kvd.common.KvdException;
import kvd.common.Utils;

public interface KvdOperations {

  /**
   * Put a new value or replace an existing.
   * @param key key with which the specified value is to be associated
   * @return {@code Future} that evaluates either to an {@code OutputStream} to be used to stream the value in.
   *         or fails (e.g. on optimistic lock or deadlock).
   *         Close the {@code OutputStream} to signal that the value is complete.
   */
  Future<OutputStream> putAsync(byte[] key);

  /**
   * Returns the value to which the specified key is mapped
   * @param key the key whose associated value is to be returned
   * @return {@code Future} that evaluates either to an {@code InputStream} for keys that exist
   *         or {@code null} for keys that don't exist on the server.
   */
  Future<InputStream> getAsync(byte[] key);

  /**
   * The returned {@code Future} evaluates to true if the key exists on the server, false otherwise
   * @param key The key whose presence is to be tested
   * @return {@code Future} evaluates to {@code true} if the key exists on the server, {@code false} otherwise
   */
  Future<Boolean> containsAsync(byte[] key);

  /**
   * Removes the mapping for the specified key from the server.
   * @param key key whose mapping is to be removed
   * @return {@code Future} which evaluates to {@code true} if the key/value was removed from the server,
   *         {@code false} otherwise.
   */
  Future<Boolean> removeAsync(byte[] key);

  /**
   * Put a new value or replace an existing.
   * @param key key with which the specified value is to be associated
   * @return {@code Future} that evaluates either to an {@code OutputStream} to be used to stream the value in.
   *         or fails (e.g. on optimistic lock or deadlock).
   *         Close the {@code OutputStream} to signal that the value is complete.
   */
  default Future<OutputStream> putAsync(String key) {
    return putAsync(key.getBytes());
  }

  /**
   * Put a new value or replace an existing.
   * @param key key with which the specified value is to be associated
   * @return {@code OutputStream} to be used to stream the value in.
   *         Close the {@code OutputStream} to signal that the value is complete.
   */
  default OutputStream put(String key) {
    try {
      return putAsync(key).get();
    } catch(Exception e) {
      throw new KvdException("put failed", e);
    }
  }

  /**
   * Put a new value or replace an existing.
   * @param key key with which the specified value is to be associated
   * @return {@code OutputStream} to be used to stream the value in.
   *         Close the {@code OutputStream} to signal that the value is complete.
   */
  default OutputStream put(byte[] key) {
    try {
      return putAsync(key).get();
    } catch(Exception e) {
      throw new KvdException("put failed", e);
    }
  }

  /**
   * Returns the value to which the specified key is mapped
   * @param key the key whose associated value is to be returned
   * @return {@code Future} that evaluates either to an {@code InputStream} for keys that exist
   *         or {@code null} for keys that don't exist on the server.
   */
  default Future<InputStream> getAsync(String key) {
    return getAsync(key.getBytes());
  }

  /**
   * Convenience method that calls {@link #getAsync(String)} and waits for the {@code Future} to complete.
   * @param key key the key whose associated value is to be returned
   * @return the {@code InputStream} for keys that exist or {@code null} for keys that don't exist on the server.
   */
  default InputStream get(String key) {
    try {
      return getAsync(key).get();
    } catch(Exception e) {
      throw new KvdException("get failed", e);
    }
  }

  /**
   * Convenience method that calls {@link #getAsync(String)} and waits for the {@code Future} to complete.
   * @param key key the key whose associated value is to be returned
   * @return the {@code InputStream} for keys that exist or {@code null} for keys that don't exist on the server.
   */
  default InputStream get(byte[] key) {
    try {
      return getAsync(key).get();
    } catch(Exception e) {
      throw new KvdException("get failed", e);
    }
  }

  /**
   * Convenience method that puts a {@code String} value.
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key. {@code null} values are not supported
   * @param charsetName the name of the requested charset, {@code null} means platform default
   */
  default void putString(String key, String value, String charsetName) {
    if(value == null) {
      throw new KvdException("null value not supported");
    }
    try(OutputStream out = put(key)) {
      out.write(value.getBytes(Utils.toCharset(charsetName)));
    } catch(IOException e) {
      throw new KvdException("put string failed", e);
    }
  }

  /**
   * Convenience method that puts a {@code String} value. Uses platform default charset
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key. {@code null} values are not supported
   **/
  default void putString(String key, String value) {
    putString(key, value, null);
  }

  /**
   * Convenience method that gets a {@code String} value.
   * @param key the key whose associated value is to be returned
   * @param charsetName the name of the requested charset, {@code null} means platform default
   * @return {@code String} value that is associated with the key or {@code null}
   *         if the key does not exist on the server.
   */
  default String getString(String key, String charsetName) {
    InputStream i = get(key);
    if(i != null) {
      try {
        byte[] buf = Utils.toByteArray(i);
        return new String(buf, Utils.toCharset(charsetName));
      } catch(IOException e) {
        throw new KvdException("getString failed", e);
      }
    } else {
      return null;
    }
  }

  /**
   * Convenience method that gets a {@code String} value.
   * @param key the key whose associated value is to be returned
   * @return {@code String} value that is associated with the key or {@code null}. Uses platform default charset
   *         if the key does not exist on the server.
   */
  default String getString(String key) {
    return getString(key, null);
  }

  /**
   * The returned {@code Future} evaluates to true if the key exists on the server, false otherwise
   * @param key The key whose presence is to be tested
   * @return {@code Future} evaluates to {@code true} if the key exists on the server, {@code false} otherwise
   */
  default Future<Boolean> containsAsync(String key) {
    return containsAsync(key.getBytes());
  }

  /**
   * Returns true if a mapping for the specified key exists on the server.
   * @param key the key whose presence is to be tested
   * @return {@code true} if the key exists on the server, {@code false} otherwise.
   */
  default boolean contains(String key) {
    try {
      return containsAsync(key).get();
    } catch(Exception e) {
      throw new KvdException("contains failed", e);
    }
  }

  /**
   * Returns true if a mapping for the specified key exists on the server.
   * @param key the key whose presence is to be tested
   * @return {@code true} if the key exists on the server, {@code false} otherwise.
   */
  default boolean contains(byte[] key) {
    try {
      return containsAsync(key).get();
    } catch(Exception e) {
      throw new KvdException("contains failed", e);
    }
  }

  /**
   * Removes the mapping for the specified key from the server.
   * @param key key whose mapping is to be removed
   * @return {@code Future} which evaluates to {@code true} if the key/value was removed from the server,
   *         {@code false} otherwise.
   */
  default Future<Boolean> removeAsync(String key) {
    return removeAsync(key.getBytes());
  }

  /**
   * Removes the mapping for the specified key from the server.
   * @param key key key whose mapping is to be removed
   * @return {@code true} if the key/value was removed from the server, {@code false} otherwise.
   */
  default boolean remove(String key) {
    try {
      return removeAsync(key).get();
    } catch(Exception e) {
      throw new KvdException("remove failed", e);
    }
  }

  /**
   * Removes the mapping for the specified key from the server.
   * @param key key key whose mapping is to be removed
   * @return {@code true} if the key/value was removed from the server, {@code false} otherwise.
   */
  default boolean remove(byte[] key) {
    try {
      return removeAsync(key).get();
    } catch(Exception e) {
      throw new KvdException("remove failed", e);
    }
  }

  /**
   * Put a byte array key/value pair.
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key. {@code null} values are not supported
   */
  default void putBytes(byte[] key, byte[] value) {
    if(key == null) {
      throw new KvdException("null key not supported");
    }
    if(key.length == 0) {
      throw new KvdException("empty key not supported");
    }
    if(value == null) {
      throw new KvdException("null value not supported");
    }
    try(OutputStream out = put(key)) {
      out.write(value);
    } catch(IOException e) {
      throw new KvdException("put bytes failed", e);
    }
  }

  /**
   * Get a {@code byte[]} value.
   * @param key the key whose associated value is to be returned
   * @return {@code byte[]} value that is associated with the key or {@code null}
   *         if the key does not exist on the server.
   */
  default byte[] getBytes(byte[] key) {
    InputStream i = get(key);
    if(i != null) {
      try {
        return Utils.toByteArray(i);
      } catch(IOException e) {
        throw new KvdException("getBytes failed", e);
      }
    } else {
      return null;
    }
  }

}
