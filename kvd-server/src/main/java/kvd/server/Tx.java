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
package kvd.server;

import kvd.server.storage.Transaction;

public class Tx {

  private int txId;

  private int channel;

  private Transaction transaction;

  public Tx(int txId, int channel, Transaction transaction) {
    super();
    this.txId = txId;
    this.channel = channel;
    this.transaction = transaction;
  }

  public int getTxId() {
    return txId;
  }

  public int getChannel() {
    return channel;
  }

  public Transaction getTransaction() {
    return transaction;
  }

  @Override
  public String toString() {
    return "Tx [txId=" + txId + ", channel=" + channel + ", transaction=" + transaction + "]";
  }

}
