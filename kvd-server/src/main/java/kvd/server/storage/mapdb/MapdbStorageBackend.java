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
package kvd.server.storage.mapdb;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.server.storage.AbstractStorageBackend;
import kvd.server.storage.Transaction;
import kvd.server.util.HumanReadableBytes;

public class MapdbStorageBackend extends AbstractStorageBackend {

  private static final Logger log = LoggerFactory.getLogger(MapdbStorageBackend.class);

  private MapdbStorage store;

  private AtomicInteger txHandles = new AtomicInteger(1);

  private int blobThreshold;

  private long blobSplitSize;

  public MapdbStorageBackend(
      File base,
      long blobThreshold,
      long blobSplitSize,
      boolean enableMmap) {
    super();
    if(blobThreshold < 0) {
      throw new KvdException("blob threshold needs to be positive int");
    }
    if(blobThreshold > 1072693248l) {
      throw new KvdException("blob threshold needs to be <= 1072693248 bytes");
    }
    this.store = new MapdbStorage(base, enableMmap);
    this.blobThreshold = (int)blobThreshold;
    this.blobSplitSize = blobSplitSize;
    log.info("blob threshold {}/{}, split at {}/{}",
        HumanReadableBytes.formatSI(blobThreshold),
        HumanReadableBytes.formatBin(blobThreshold),
        HumanReadableBytes.formatSI(blobSplitSize),
        HumanReadableBytes.formatBin(blobSplitSize));
  }

  @Override
  public Transaction begin() {
    txHandles.compareAndSet(Integer.MAX_VALUE, 1);
    int txHandle = txHandles.getAndIncrement();
    return new MapdbTx(txHandle, store, blobThreshold, blobSplitSize);
  }

  public MapdbStorage getStore() {
    return store;
  }

}
