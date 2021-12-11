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
package kvd.server.storage.fs;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.server.storage.AbstractStorageBackend;
import kvd.server.storage.Transaction;
import kvd.server.storage.trash.Trash;
import kvd.server.util.FileUtils;

public class FileStorageBackend extends AbstractStorageBackend {

  private static final Logger log = LoggerFactory.getLogger(FileStorageBackend.class);

  public File base;

  public File storage;

  public File txBase;

  private Map<Integer, FileTx> transactions = Collections.synchronizedMap(new HashMap<>());

  private FileStorage store;

  private AtomicInteger txHandles = new AtomicInteger(1);

  public FileStorageBackend(File base, Trash trash) {
    super();
    this.base = base;
    this.storage = new File(base, "store");
    this.txBase = new File(base, "transactions");
    FileUtils.createDirIfMissing(base);
    FileUtils.createDirIfMissing(storage);
    FileUtils.createDirIfMissing(txBase);
    checkVersion(base);
    store = new FileStorage(storage, trash);
    start();
  }

  private void checkVersion(File base) {
    File version = new File(base, "version");
    if(version.exists()) {
      try {
        String v = StringUtils.strip(org.apache.commons.io.FileUtils.readFileToString(version, StandardCharsets.UTF_8));
        if(!StringUtils.equals("1", v)) {
          throw new KvdException("file store on disk not supported, version '" + v + "'");
        }
      } catch (IOException e) {
        throw new KvdException("failed to read version file", e);
      }
    } else {
      try {
        org.apache.commons.io.FileUtils.writeStringToFile(version, "1", StandardCharsets.UTF_8);
      } catch (IOException e) {
        throw new KvdException("failed to write version", e);
      }
    }
  }

  private synchronized void start() {
    Arrays.stream(txBase.listFiles()).forEach(f -> {
      Integer txId = toInt(f.getName());
      if(txId != null) {
        try(FileTx tx = new FileTx(txId, store, () -> {}, f)) {
          log.debug("found tx '{}' on startup, closed '{}'{}", txId, tx.isClosed(), tx.isClosed()?"":", rollback");
          tx.rollback();
        }
      }
    });
  }

  private Integer toInt(String s) {
    try {
      return Integer.valueOf(s);
    } catch(Exception e) {
      return null;
    }
  }

  @Override
  public synchronized Transaction begin() {
    txHandles.compareAndSet(Integer.MAX_VALUE, 1);
    int txHandle = txHandles.getAndIncrement();
    File fTx = new File(txBase, ""+txHandle);
    FileUtils.deleteDirQuietly(fTx);
    FileUtils.createDirIfMissing(fTx);
    FileTx tx = new FileTx(txHandle, store, () -> transactions.remove(txHandle), fTx);
    transactions.put(txHandle, tx);
    return tx;
  }

}
