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
package kvd.server.storage.concurrent;

import kvd.common.KvdException;

public class OptimisticLockException extends KvdException {

  private static final long serialVersionUID = -7120339264127480357L;

  public OptimisticLockException() {
    super();
  }

  public OptimisticLockException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public OptimisticLockException(String message, Throwable cause) {
    super(message, cause);
  }

  public OptimisticLockException(String message) {
    super(message);
  }

  public OptimisticLockException(Throwable cause) {
    super(cause);
  }

}
