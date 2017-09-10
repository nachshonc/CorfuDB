package org.corfudb.runtime.object.transactions;

/** This class is deprecated - please use {@Class ReadAfterWriteTransactionalContext}
 *  instead.
 */
@Deprecated
public class OptimisticTransactionalContext extends ReadAfterWriteTransactionalContext {

    /** Generate a new optimistic (read-after-write) transactional context. */
    public OptimisticTransactionalContext(TransactionBuilder builder) {
        super(builder);
    }
}
