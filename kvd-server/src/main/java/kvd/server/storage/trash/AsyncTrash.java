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
package kvd.server.storage.trash;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;

public class AsyncTrash implements Trash {

  private static final Logger log = LoggerFactory.getLogger(AsyncTrash.class);

  private File trashDir;

  private Thread trashCollector;

  public AsyncTrash(File trashDir) {
    super();
    this.trashDir = trashDir;
    trashCollector = new Thread(this::trashCollectorLoop, "trash-collector");
    trashCollector.setDaemon(true);
    trashCollector.start();
  }

  @Override
  public void remove(File f) {
    if(f == null) {
      return;
    }
    if(!f.exists()) {
      return;
    }
    File dest = new File(trashDir, UUID.randomUUID().toString());
    try {
      FileUtils.forceMkdir(trashDir);
      Files.move(f.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      throw new KvdException(String.format("failed to move tx file to store, src '%s', dest '%s'",
          f.getAbsolutePath(), dest.getAbsolutePath()), e);
    }
    trashCollectNotify();
  }

  private void trashCollectorLoop() {
    for(;;) {
      try {
        collectTrash();
      } catch(Throwable t) {
        log.error("error in trash collection", t);
      }
      try {
        FileUtils.forceMkdir(trashDir);
        trashCollectWait();
      } catch(Throwable t) {
        log.warn("trash collect wait with exception", t);
      }
    }
  }

  private void collectTrash() {
    TrashUtils.deleteAllButRootDirectory(trashDir);
  }

  private synchronized void trashCollectWait() throws InterruptedException {
    wait(TimeUnit.MINUTES.toMillis(1));
  }

  private synchronized void trashCollectNotify() {
    notify();
  }

}
