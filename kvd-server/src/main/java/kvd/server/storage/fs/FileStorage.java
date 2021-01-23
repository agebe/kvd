/*
 * Copyright 2020 Andre Gebers
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
package kvd.server.storage.fs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.server.storage.AbortableOutputStream;
import kvd.server.storage.KeyUtils;
import kvd.server.storage.StorageBackend;

public class FileStorage implements StorageBackend {

  private static final Logger log = LoggerFactory.getLogger(FileStorage.class);

  private static class Staging {

    private String key;

    private File f;

    public Staging(String key, File f) {
      super();
      this.key = key;
      this.f = f;
    }

    public String getKey() {
      return key;
    }

    public File getFile() {
      return f;
    }
  }

  public File base;

  public File storage;

  public File staging;

  private Map<String, Staging> wmap = new HashMap<>();

  public FileStorage(File base) {
    super();
    this.base = base;
    this.storage = new File(base, "store");
    this.staging = new File(base, "staging");
    createDirIfMissing(base);
    createDirIfMissing(storage);
    createDirIfMissing(staging);
  }

  private void createDirIfMissing(File d) {
    if(!d.exists()) {
      if(!d.mkdirs()) {
        throw new KvdException(String.format("'%s' does not exist and failed to create", d.getAbsolutePath()));
      }
    }
    if(!d.isDirectory()) {
      throw new KvdException(String.format("'%s' exists but not directory", d.getAbsolutePath()));
    }
    // TODO write test
  }

  @Override
  public synchronized AbortableOutputStream put(String key) {
    String txId = UUID.randomUUID().toString();
    File f = new File(staging, txId);
    try {
      AbortableOutputStream out = new AbortableOutputStream(
          new BufferedOutputStream(new FileOutputStream(f)),
          txId,
          this::commit,
          this::rollback);
      Staging staging = new Staging(KeyUtils.internalKey(key), f);
      wmap.put(txId, staging);
      log.info("starting put, key '{}', tx '{}'", staging.getKey(), txId);
      return out;
    } catch(Exception e) {
      throw new KvdException("failed to create staging file for key " + key, e);
    }
  }

  private synchronized void commit(String txId) {
    Staging staging = wmap.get(txId);
    try {
      if(staging != null) {
        Path to = new File(storage, staging.getKey()).toPath();
        Path from = staging.getFile().toPath();
        Files.move(from, to, StandardCopyOption.REPLACE_EXISTING);
      } else {
        log.warn("staging file missing, {}", txId);
      }
    } catch(Exception e) {
      rollback(txId);
      throw new KvdException("failed to finish put " + txId);
    } finally {
      wmap.remove(txId);
    }
  }

  private synchronized void rollback(String txId) {
    Staging staging = wmap.get(txId);
    if(staging != null) {
      File f = staging.getFile();
      if(f.exists()) {
        if(!f.delete()) {
          log.warn("delete failed for '{}' on abort, {}", f.getAbsolutePath(), txId);
        }
      }
    }
  }

  @Override
  public InputStream get(String key) {
    try {
      String s = KeyUtils.internalKey(key);
      File f = new File(storage, s);
      if(f.canRead()) {
        return new BufferedInputStream(new FileInputStream(f));
      } else {
        return null;
      }
    } catch(Exception e) {
      throw new KvdException("failed to get key " + key, e);
    }
  }

  @Override
  public boolean contains(String key) {
    String s = KeyUtils.internalKey(key);
    File f = new File(storage, s);
    return f.canRead();
  }

  @Override
  public boolean remove(String key) {
    String s = KeyUtils.internalKey(key);
    File f = new File(storage, s);
    if(f.exists()) {
      boolean removed = f.delete();
      if(!removed) {
        log.warn("file '{}' could not be removed", f.getAbsolutePath());
      }
      return removed;
    } else {
      return false;
    }
  }

}
