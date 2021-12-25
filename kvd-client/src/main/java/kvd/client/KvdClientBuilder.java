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
package kvd.client;

import java.util.concurrent.TimeUnit;

import kvd.common.KvdException;

/**
 * Use this builder to construct a {@link KvdClient} instance when you need to set configuration options
 * other than the default. Anticipating more options in the future.
 */
public class KvdClientBuilder {

  private long transactionDefaultTimeoutMs = TimeUnit.MINUTES.toMillis(1);

  private String serverAddress;

  private int socketSoTimeoutMs = (int)TimeUnit.MINUTES.toMillis(1);

  private int serverTimeoutSeconds = 60;

  public KvdClientBuilder() {
    super();
  }

  KvdClientBuilder(String serverAddress) {
    this.serverAddress = serverAddress;
  }

  /**
   * Set the default transaction timeout in milliseconds, 0 means no timeout.
   */
  public KvdClientBuilder setTransactionDefaultTimeoutMs(long transactionDefaultTimeoutMs) {
    if(transactionDefaultTimeoutMs < 0) {
      throw new KvdException("invalid transactionDefaultTimeoutMs, "+ transactionDefaultTimeoutMs);
    }
    this.transactionDefaultTimeoutMs = transactionDefaultTimeoutMs;
    return this;
  }

  public KvdClientBuilder setSocketSoTimeoutMs(int socketSoTimeoutMs) {
    this.socketSoTimeoutMs = socketSoTimeoutMs;
    return this;
  }

  public KvdClientBuilder setServerTimeoutSeconds(int serverTimeoutSeconds) {
    this.serverTimeoutSeconds = serverTimeoutSeconds;
    return this;
  }

  /**
   * Create the KvdClient connecting to the given server.
   * @param serverAddress The serverAddress is in the form 
   * <a href="https://guava.dev/releases/30.1-jre/api/docs/com/google/common/net/HostAndPort.html">{@code host:port}</a>
   * @return The {@link KvdClient} instance
   */
  public KvdClient create(String serverAddress) {
    this.serverAddress = serverAddress;
    return new KvdClient(this);
  }

  long getTransactionDefaultTimeoutMs() {
    return transactionDefaultTimeoutMs;
  }

  String getServerAddress() {
    return serverAddress;
  }

  int getSocketSoTimeoutMs() {
    return socketSoTimeoutMs;
  }

  int getServerTimeoutSeconds() {
    return serverTimeoutSeconds;
  }

}
