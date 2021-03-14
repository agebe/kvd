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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;

class FileStorage {

  private static final Logger log = LoggerFactory.getLogger(FileStorage.class);

  private File storage;

  public FileStorage(File storage) {
    super();
    this.storage = storage;
  }

  synchronized void commit(FileTx tx) {
    tx.storeCommit();
  }

  InputStream get(String key) {
    File f = new File(storage, key);
    try {
      return f.exists()?new BufferedInputStream(new FileInputStream(f)):null;
    } catch(Exception e) {
      throw new KvdException(String.format("failed to read '%s' from file store", key), e);
    }
  }

  boolean contains(String key) {
    File f = new File(storage, key);
    return f.exists();
  }

  void remove(String key) {
    File f = new File(storage, key);
    f.delete();
  }

  void moveToStore(String key, File src) {
    File dest = new File(storage, key);
    try {
      Files.move(src.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      log.error("failed to move tx file to store, src '{}', dest '{}'", src.getAbsolutePath(), dest.getAbsolutePath());
    }
  }

}
