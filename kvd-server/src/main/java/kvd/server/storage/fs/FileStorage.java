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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
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
    return getContent(key, new File(storage, key));
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

  // get the content only, remove the header if present
  static InputStream getContent(String key, File f) {
    try {
      InputStream i = f.exists()?new BufferedInputStream(new FileInputStream(f)):null;
      if(i == null) {
        return null;
      }
      if(StringUtils.startsWith(key, "01")) {
        // hashed file name contains a header that needs to be skipped here.
        byte[] headerLength = new byte[4];
        IOUtils.readFully(i, headerLength);
        int length = ByteBuffer.wrap(headerLength).order(ByteOrder.LITTLE_ENDIAN).getInt();
        if(length <= 0) {
          throw new KvdException(String.format("wrong header length '%s' on file '%s'", length, f.getAbsolutePath()));
        }
        IOUtils.skip(i, length);
      }
      return i;
    } catch(Exception e) {
      throw new KvdException(String.format("failed to read from file '%s'", f.getAbsolutePath()), e);
    }
  }

  void removeAll() {
    try {
      // from https://stackoverflow.com/a/27917071
      Files.walkFileTree(storage.toPath(), new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          Files.delete(file);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          if(!dir.equals(storage.toPath())) {
            Files.delete(dir);
          }
          return FileVisitResult.CONTINUE;
        }
      });
    } catch(IOException e) {
      throw new KvdException("failed to remove all" , e);
    }
  }

}
