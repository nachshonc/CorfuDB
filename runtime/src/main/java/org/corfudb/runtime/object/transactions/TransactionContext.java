package org.corfudb.runtime.object.transactions;

import lombok.Getter;

/** A transaction context contains all the thread-local transaction context
 *  for a given thread. This includes the write set, the conflict set and
 *  the snapshot address.
 */
public class TransactionContext {

    @Getter
    final WriteSetInfo writeSet = new WriteSetInfo();

    @Getter
    final ConflictSetInfo conflictSet = new ConflictSetInfo();

    @Getter
    long optimisticSnapshot;
}
