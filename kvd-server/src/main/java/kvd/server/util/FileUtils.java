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
package kvd.server.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

import kvd.common.KvdException;

public class FileUtils {

  public static void createDirIfMissing(File d) {
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

  public static void deleteDirQuietly(File d) {
    try(Stream<Path> stream = Files.walk(d.toPath())) {
      stream
      .sorted(Comparator.reverseOrder())
      .map(Path::toFile)
      .forEach(File::delete);
    } catch (IOException e) {
      // ignore
    }
  }

}
