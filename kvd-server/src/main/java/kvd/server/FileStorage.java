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
package kvd.server;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.common.Utils;

public class FileStorage implements Storage {

  private static final Logger log = LoggerFactory.getLogger(FileStorage.class);

  private static class Staging {

    private String key;
    private File tmpFile;
    private OutputStream out;
    public Staging(String key, File tmpFile) {
      super();
      try {
        this.key = key;
        this.tmpFile = tmpFile;
        this.out = new BufferedOutputStream(new FileOutputStream(tmpFile));
      } catch(Exception e) {
        throw new KvdException("failed to create staging file for key " + key, e);
      }
    }
    public String getKey() {
      return key;
    }
    public File getTmpFile() {
      return tmpFile;
    }
    public OutputStream getOut() {
      return out;
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
  public synchronized OutputStream begin(String key) {
    File f = new File(staging, UUID.randomUUID().toString());
    Staging staging = new Staging(KeyUtils.internalKey(key), f);
    wmap.put(staging.getKey(), staging);
    return staging.getOut();
  }

  @Override
  public synchronized void finish(String key) {
    String s = KeyUtils.internalKey(key);
    Staging staging = wmap.get(s);
    try {
      if(staging != null) {
        staging.getOut().close();
        Path to = new File(storage, s).toPath();
        Path from = staging.getTmpFile().toPath();
        Files.move(from, to, StandardCopyOption.REPLACE_EXISTING);
      } else {
        log.warn("staging file for key '{}' missing", key);
      }
    } catch(Exception e) {
      abort(key);
      throw new KvdException("failed to finish put of " + key);
    } finally {
      wmap.remove(s);
    }
  }

  @Override
  public void abort(String key) {
    String s = KeyUtils.internalKey(key);
    Staging staging = wmap.get(s);
    if(staging != null) {
      Utils.closeQuietly(staging.getOut());
      File f = staging.getTmpFile();
      if(f.exists()) {
        if(!staging.getTmpFile().delete()) {
          log.warn("delete failed for '{}' on abort", f.getAbsolutePath());
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
    boolean removed = f.delete();
    if(!removed) {
      log.warn("file '{}' could not be removed", f.getAbsolutePath());
    }
    return removed;
  }

}
