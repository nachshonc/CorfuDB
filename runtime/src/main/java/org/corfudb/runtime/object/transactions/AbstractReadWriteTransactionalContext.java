package org.corfudb.runtime.object.transactions;

public abstract class AbstractReadWriteTransactionalContext
        extends AbstractTransactionalContext {

    public AbstractReadWriteTransactionalContext(TransactionBuilder builder) {
        super(builder);
    }



}
