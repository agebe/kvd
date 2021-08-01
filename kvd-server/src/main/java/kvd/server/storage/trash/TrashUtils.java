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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import kvd.common.KvdException;

class TrashUtils {

  static void deleteAll(File f) {
    delete(f, true);
  }

  static void deleteAllButRootDirectory(File f) {
    delete(f, false);
  }

  private static void delete(File f, boolean deleteRoot) {
    try {
      if(f.exists()) {
        if(f.isDirectory()) {
          deleteDirectory(f, deleteRoot);
        } else if(f.isFile()) {
          Files.delete(f.toPath());
        }
      }
    } catch(IOException e) {
      throw new KvdException("failed to delete " + f.getAbsolutePath(), e);
    }
  }

  private static void deleteDirectory(File root, boolean deleteRoot) throws IOException {
    // from https://stackoverflow.com/a/27917071
    Files.walkFileTree(root.toPath(), new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        if(deleteRoot || !root.toPath().equals(dir)) {
          Files.delete(dir);
        }
        return FileVisitResult.CONTINUE;
      }
    });
  }

}
