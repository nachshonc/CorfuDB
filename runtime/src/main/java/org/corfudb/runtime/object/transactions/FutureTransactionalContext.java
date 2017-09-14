package org.corfudb.runtime.object.transactions;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.exceptions.TransactionAbortedException;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.corfudb.runtime.view.ObjectsView.TRANSACTION_STREAM_ID;

@Slf4j
public class FutureTransactionalContext extends OptimisticTransactionalContext {

    FutureTransactionalContext(TransactionBuilder builder) {
        super(builder);
    }

    Set<CompletableFuture> futureSet = new HashSet<>();
    Set<UUID> futureAffectedStream = new HashSet<>();


    public void addFuture(CompletableFuture f, UUID uuid){
        futureSet.add(f);
        futureAffectedStream.add(uuid);
    }

    public long getConflictSetAndCommit(ConflictSetInfo conflictSet) {


        if (TransactionalContext.isInNestedTransaction()) {
            getParentContext().addTransaction(this);
            commitAddress = AbstractTransactionalContext.FOLDED_ADDRESS;
            log.trace("Commit[{}] Folded into {}", this, getParentContext());
            return commitAddress;
        }

        // If the write set is empty, we're done and just return
        // NOWRITE_ADDRESS.
        if (getWriteSetInfo().getWriteSet().getEntryMap().isEmpty()) {
            log.trace("Commit[{}] Read-only commit (no write)", this);
            return NOWRITE_ADDRESS;
        }

        // Write to the transaction stream if transaction logging is enabled
        Set<UUID> affectedStreams = new HashSet<>(getWriteSetInfo().getWriteSet()
                .getEntryMap().keySet());
        if (this.builder.runtime.getObjectsView().isTransactionLogging()) {
            affectedStreams.add(TRANSACTION_STREAM_ID);
        }

        affectedStreams.addAll(futureAffectedStream);

        // Now we obtain a conditional address from the sequencer.
        // This step currently happens all at once, and we get an
        // address of -1L if it is rejected.
        long address = -1L;

        TokenResponse response = this.builder.runtime.getStreamsView().append1( // a set of stream-IDs that contains the affected streams
                affectedStreams,
                null);

        long currentVersion = response.getTokenValue();

        setSnapshotTimestamp(currentVersion-1);
        for(CompletableFuture f : futureSet){
            try {
                f.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        address = this.builder.runtime.getStreamsView().append2(

                        // a set of stream-IDs that contains the affected streams
                        affectedStreams,

                        // a MultiObjectSMREntry that contains the update(s) to objects
                        collectWriteSetEntries(),

                        // TxResolution info:
                        // 1. snapshot timestamp
                        // 2. a map of conflict params, arranged by streamID's
                        // 3. a map of write conflict-params, arranged by
                        // streamID's
                        new TxResolutionInfo(getTransactionID(),
                                getSnapshotTimestamp(),
                                conflictSet.getHashedConflictSet(),
                                getWriteSetInfo().getHashedConflictSet()),


                        response
                );

        log.trace("Commit[{}] Acquire address {}", this, address);

        super.superCommitTransaction();
        commitAddress = address;

        tryCommitAllProxies();
        log.trace("Commit[{}] Written to {}", this, address);
        return address;
    }
}
