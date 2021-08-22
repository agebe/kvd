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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;

import kvd.common.KvdException;
import kvd.common.Utils;
import kvd.server.Key;
import kvd.server.storage.AbortableOutputStream;
import kvd.server.storage.AbstractTransaction;

class FileTx extends AbstractTransaction {

  private static final Logger log = LoggerFactory.getLogger(FileTx.class);

  static final String COMMIT_MARKER = "commit";
  static final String REMOVE = "remove";

  private FileStorage store;

  private Runnable closeListener;

  private File fTx;

  private File fTxStore;

  private File fTxStage;

  private Map<String, Staging> staging = new HashMap<>();

  private Set<String> txRemove = new HashSet<>();

  private ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock();
  private Lock rlock = rwlock.readLock();
  private Lock wlock = rwlock.writeLock();

  private boolean crashOnCommitTest;

  @SuppressWarnings("unchecked")
  public FileTx(int handle, FileStorage store, Runnable closeListener, File fTx) {
    super(handle);
    this.store = store;
    this.closeListener = closeListener;
    this.fTx = fTx;
    fTxStage = new File(fTx, "stage");
    fTxStore = new File(fTx, "store");
    FileUtils.createDirIfMissing(fTxStage);
    FileUtils.createDirIfMissing(fTxStore);
    File fRemove = new File(fTx, REMOVE);
    if(fRemove.exists()) {
      try(ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(new FileInputStream(fRemove)))) {
        txRemove = (Set<String>)in.readObject();
      } catch(Exception e) {
        log.warn("failed to load remove key set", e);
      }
    }
    File fCommitMarker = new File(fTx, COMMIT_MARKER);
    if(fCommitMarker.exists()) {
      log.info("redo commit of tx '{}'", handle);
      commit();
    }
  }

  private void cleanup() {
    FileUtils.deleteDirQuietly(fTx);
  }

  private void prepareCommit() {
    File marker = new File(fTx, COMMIT_MARKER);
    try {
      marker.createNewFile();
    } catch(IOException e) {
      throw new KvdException("failed to create commit marker", e);
    }
    try(ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(
        new FileOutputStream(new File(fTx, REMOVE))))) {
      out.writeObject(txRemove);
    } catch(IOException e) {
      throw new KvdException("failed to save remove key set", e);
    }
  }

  void setCrashOnCommitTest() {
    crashOnCommitTest = true;
  }

  void storeCommit() {
    prepareCommit();
    if(crashOnCommitTest) {
      throw new KvdException("crash on commit test enabled");
    }
    txRemove.stream().forEach(store::remove);
    Arrays.stream(fTxStore.listFiles()).forEach(src -> {
      if(src.isFile()) {
        store.moveToStore(src.getName(), src);
      }
    });
  }

  @Override
  protected void commitInternal() {
    wlock.lock();
    try {
      closeListener.run();
      store.commit(this);
      abortUnfinishedPuts();
      cleanup();
    } finally {
      wlock.unlock();
    }
  }

  @Override
  protected void rollbackInternal() {
    wlock.lock();
    try {
      closeListener.run();
      abortUnfinishedPuts();
      cleanup();
    } finally {
      wlock.unlock();
    }
  }

  private void abortUnfinishedPuts() {
    new ArrayList<Staging>(staging.values()).forEach(staging -> {
      log.warn("aborting unfinished put '{}'", staging.getKey());
      staging.abort();
    });
    staging.clear();
  }

  private String internalKey(byte[] key) {
    Utils.checkKey(key);
    if(key.length > 100) {
      return "01" + Hashing.sha256().hashBytes(key).toString();
    } else {
      return "00" + BaseEncoding.base16().lowerCase().encode(key);
    }
  }

  @Override
  public AbortableOutputStream<?> put(Key key) {
    checkClosed();
    String internalKey = null;
    wlock.lock();
    try {
      internalKey = internalKey(key.getBytes());
      String stageId = UUID.randomUUID().toString();
      File file = new File(fTxStage, stageId);
      AbortableOutputStream<String> out = new AbortableOutputStream<>(
          new BufferedOutputStream(new FileOutputStream(file)),
          stageId,
          this::putCommit,
          this::putRollback);
      if(StringUtils.startsWith(internalKey, "01")) {
        // we have a hashed key, store the original key inside the file so it can be later recovered
        byte[] lengthHeader = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(key.getBytes().length+1).array();
        out.write(lengthHeader);
        // version
        out.write(1);
        out.write(key.getBytes());
      }
      Staging staging = new Staging(internalKey, file, out);
      this.staging.put(stageId, staging);
      log.debug("starting put, key '{}', tx '{}'", internalKey, stageId);
      return out;
    } catch(Exception e) {
      // TODO error handling
      throw new KvdException("failed start put", e);
    } finally {
      wlock.unlock();
    }
  }

  private void putCommit(String stageId) {
    wlock.lock();
    try {
      Staging staging = this.staging.remove(stageId);
      if(staging != null) {
        File from = staging.getFile();
        if(!from.exists()) {
          log.warn("put commit src '{}' does not exist", from.getAbsolutePath());
        }
        File to = new File(fTxStore, staging.getKey());
        log.trace("put commit, move from '{}' to '{}'", from.getAbsolutePath(), to.getAbsolutePath());
        Files.move(from.toPath(), to.toPath(), StandardCopyOption.REPLACE_EXISTING);
        txRemove.remove(staging.getKey());
      } else {
        log.warn("unknown put with staging id '{}'", stageId);
      }
    } catch(Exception e) {
      log.warn("putCommit failed '{}'", stageId, e);
      putRollback(stageId);
    } finally {
      wlock.unlock();
    }
  }

  private void putRollback(String txId) {
    wlock.lock();
    try {
      Staging staging = this.staging.remove(txId);
      if((staging != null) && (staging.getFile() != null)) {
        staging.getFile().delete();
      }
    } finally {
      wlock.unlock();
    }
  }

  @Override
  public InputStream get(Key key) {
    checkClosed();
    String internalKey = null;
    rlock.lock();
    try {
      internalKey = internalKey(key.getBytes());
      File f = new File(fTxStore, internalKey);
      if(f.exists()) {
        return FileStorage.getContent(internalKey, f);
      } else {
        return store.get(internalKey);
      }
    } catch(Exception e) {
      throw new KvdException(String.format("failed to get '%s'", internalKey), e);
    } finally {
      rlock.unlock();
    }
  }

  @Override
  public boolean contains(Key key) {
    checkClosed();
    rlock.lock();
    try {
      String internalKey = internalKey(key.getBytes());
      File f = new File(fTxStore, internalKey);
      if(f.exists()) {
        return true;
      } else if(txRemove.contains(internalKey)) {
        return false;
      } else {
        return store.contains(internalKey);
      }
    } finally {
      rlock.unlock();
    }
  }

  @Override
  public boolean remove(Key key) {
    checkClosed();
    wlock.lock();
    try {
      boolean contains = contains(key);
      if(contains) {
        String internalKey = internalKey(key.getBytes());
        File f = new File(fTxStore, internalKey);
        if(f.exists()) {
          f.delete();
        }
        txRemove.add(internalKey);
      }
      return contains;
    } finally {
      wlock.unlock();
    }
  }

  @Override
  public void removeAll() {
    store.removeAll();
  }
}
