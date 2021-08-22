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
package kvd.server.storage.timestamp;

import java.time.Instant;

import kvd.server.Key;

public class TimestampRecord extends Timestamp {

  private AccessType accessType;

  public TimestampRecord(Key key, long timestamp, AccessType accessType) {
    super(key, timestamp);
    this.accessType = accessType;
  }

  AccessType getAccessType() {
    return accessType;
  }

  private static long now() {
    return Instant.now().toEpochMilli();
  }

  static TimestampRecord putTimestamps(Key key) {
    long now = now();
    return new TimestampRecord(key, now, AccessType.CREATED);
  }

  static TimestampRecord readTimestamps(Key key) {
    long now = now();
    return new TimestampRecord(key, now, AccessType.ACCESSED);
  }

  static TimestampRecord removeTimestamps(Key key) {
    long now = now();
    return new TimestampRecord(key, now, AccessType.REMOVED);
  }

}
