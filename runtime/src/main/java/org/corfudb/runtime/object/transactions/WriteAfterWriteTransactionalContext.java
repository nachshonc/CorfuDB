package org.corfudb.runtime.object.transactions;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ICorfuSMRProxyInternal;

/** A write-after-write transactional context.
 *
 * <p>A write-after-write transactional context behaves like an optimistic
 * context, except behavior during commit (for writes):
 *
 *   <p>(1) Reads behave the same as in a regular optimistic
 *     transaction.
 *
 *   <p>(2) Writes in a write-after-write transaction are guaranteed
 *     to commit atomically, if and only if none of the objects
 *     written (the "write set") were modified between the first read
 *     ("first read timestamp") and the time of commit.
 *
 * <p>Created by mwei on 11/21/16.
 */
@Slf4j
public class WriteAfterWriteTransactionalContext
        extends AbstractOptimisticTransactionalContext {

    WriteAfterWriteTransactionalContext(TransactionBuilder builder) {
        super(builder);
        getSnapshotTimestamp();
    }

    @Override
    protected <T> long addToWriteSet(ICorfuSMRProxyInternal<T> proxy,
                                     SMREntry updateEntry,
                                     Object[] conflictObject) {
        TransactionalContext.getConflictSet().add(proxy, conflictObject);
        return super.addToWriteSet(proxy, updateEntry, conflictObject);
    }

}
