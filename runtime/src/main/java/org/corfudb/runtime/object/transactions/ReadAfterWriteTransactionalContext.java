package org.corfudb.runtime.object.transactions;

import org.corfudb.runtime.object.ICorfuSMRProxyInternal;

public class ReadAfterWriteTransactionalContext
        extends AbstractOptimisticTransactionalContext {

    public ReadAfterWriteTransactionalContext(TransactionBuilder builder) {
        super(builder);
    }

    @Override
    protected <T> void addToReadSet(ICorfuSMRProxyInternal<T> proxy,
                                       Object[] conflictObject) {
        Transactions.getContext().getConflictSet().add(proxy, conflictObject);
    }

}
