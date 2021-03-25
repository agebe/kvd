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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.server.storage.StorageBackend;

/**
 * Manages read/write locks and stalls transactions that can't currently proceed waiting to acquire a lock.
 * This locking system also fails the operation that causes a deadlock immediately.
 */
public class PessimisticLockStorageBackend extends AbstractLockStorageBackend {

  private static final Logger log = LoggerFactory.getLogger(PessimisticLockStorageBackend.class);

  // records hold edges between transactions and keys.
  // a vertex is either a key or a transaction
  // an edge goes between a key and a transaction only (not key to key or transaction to transaction)
  // an edge pointing always from a key to a transaction shows a hold (transaction has locked that key)
  private Graph<KeyOrTx, DefaultEdge> lockAllocationGraph = new DefaultDirectedGraph<>(DefaultEdge.class);

  // directed graph recording which transactions are waiting on each other for deadlock detection
  // The DirectedAcyclicGraph does not allow cycles in the graph so detecting deadlocks is as simple as adding
  // wait edges to the graph. if it goes bang we detected a deadlock.
  private Graph<LockTransaction, DefaultEdge> waitGraph = new DirectedAcyclicGraph<>(DefaultEdge.class);

  public PessimisticLockStorageBackend(StorageBackend backend, LockMode mode) {
    super(backend, mode);
  }

  @Override
  protected boolean canReadLockNow(LockTransaction tx, String key, Set<LockTransaction> lockHolders) {
    if(lockHolders.isEmpty()) {
      return true;
    } else {
      // check any other transaction that holds a key. only one is sufficient to check if read lock can be acquired
      LockTransaction other = lockHolders.iterator().next();
      return other.getLock(key).equals(LockType.READ);
    }
  }

  @Override
  protected boolean canWriteLockNow(LockTransaction tx, String key, Set<LockTransaction> lockHolders) {
    return lockHolders.isEmpty();
  }

  @Override
  protected boolean canWriteLockUpgradeNow(LockTransaction tx, String key, Set<LockTransaction> lockHolders) {
    if(lockHolders.contains(tx)) {
      return lockHolders.size() == 1;
    } else {
      throw new LockException("internal error on lock upgrade, current tx does not hold lock");
    }
  }

  @Override
  protected synchronized void recordHold(LockTransaction tx, String key) {
    log.trace("record hold tx '{}', key '{}'", tx, key);
    KeyOrTx vTx = new KeyOrTx(tx);
    KeyOrTx vKey = new KeyOrTx(key);
    lockAllocationGraph.addVertex(vTx);
    lockAllocationGraph.addVertex(vKey);
    // add a 'hold edge' if it not already exists
    lockAllocationGraph.addEdge(vKey, vTx);
    log.trace("recordHold, lock allocation graph '{}'", lockAllocationGraph);
    log.trace("recordHold, wait graph '{}'", waitGraph);
    log.trace("record hold done");
  }

  @Override
  protected synchronized void recordWait(LockTransaction tx, String key) {
    log.trace("record wait tx '{}', key '{}'", tx, key);
    KeyOrTx vTx = new KeyOrTx(tx);
    KeyOrTx vKey = new KeyOrTx(key);
    lockAllocationGraph.addVertex(vTx);
    lockAllocationGraph.addVertex(vKey);
    // remember which edges have been added so they can be undone in case of an exception (e.g. deadlock detected)
    List<DefaultEdge> edges = new ArrayList<>();
    try {
      for(DefaultEdge otherHoldEdge : lockAllocationGraph.outgoingEdgesOf(vKey)) {
        KeyOrTx otherHoldTx = lockAllocationGraph.getEdgeTarget(otherHoldEdge);
        if(otherHoldTx.getTx() == null) {
          throw new KvdException("lock exception, hold edge pointing to other key but expected transaction");
        } else if(otherHoldTx.getTx().equals(tx)) {
          // ignore, we don't want to wait on ourselves. I think this might happen on lock upgrades (read -> write)
          continue;
        } else {
          log.trace("found other hold lock tx '{}', adding to wait graph...", otherHoldTx.getTx().handle());
          waitGraph.addVertex(tx);
          waitGraph.addVertex(otherHoldTx.getTx());
          try {
            DefaultEdge edge = waitGraph.addEdge(tx, otherHoldTx.getTx());
            if(edge != null) {
              edges.add(edge);
            }
          } catch(IllegalArgumentException e) {
            throw new AcquireLockException(String.format("deadlock detected, '%s', key '%s'", tx, key), e);
          }
        }
      }
    } catch(Exception e) {
      // wait edge cleanup is required as the transaction is not waiting after the exception is thrown.
      waitGraph.removeAllEdges(edges);
      // vertex cleanup not required here as they (vertex) get removed on releaseAllLocks when the transaction finishes
      throw e;
    }
    log.trace("recordWait, lock allocation graph '{}'", lockAllocationGraph);
    log.trace("recordWait, wait graph '{}'", waitGraph);
    log.trace("record wait done");
  }

  @Override
  protected synchronized void releaseAllLocks(LockTransaction tx) {
    log.trace("release all locks '{}'", tx);
    super.releaseAllLocks(tx);
    waitGraph.removeVertex(tx);
    try {
      KeyOrTx vTx = new KeyOrTx(tx);
      if(lockAllocationGraph.containsVertex(vTx)) {
        Set<KeyOrTx> keys = lockAllocationGraph.incomingEdgesOf(new KeyOrTx(tx))
            .stream()
            .map(edge -> lockAllocationGraph.getEdgeSource(edge))
            .collect(Collectors.toSet());
        lockAllocationGraph.removeVertex(vTx);
        // remove keys that have no edges
        keys.forEach(vKey -> {
          if(lockAllocationGraph.degreeOf(vKey) == 0) {
            lockAllocationGraph.removeVertex(vKey);
          }
        });
      }
    } catch(Exception e) {
      log.warn("release all locks failed", e);
    }
    log.trace("release, lock allocation graph '{}'", lockAllocationGraph);
    log.trace("release, wait graph '{}'", waitGraph);
    log.trace("release all locks done");
  }

}
