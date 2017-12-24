package org.corfudb.runtime.object.transactions;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.object.ICorfuSMRAccess;
import org.corfudb.runtime.object.ICorfuSMRProxyInternal;
import org.corfudb.runtime.object.VersionLockedObject;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;

import static org.corfudb.runtime.view.ObjectsView.TRANSACTION_STREAM_ID;

@Slf4j
public class FutureTransactionalContext extends OptimisticTransactionalContext {

    FutureTransactionalContext(TransactionBuilder builder) {
        super(builder);
    }

    Set<Callable<Object>> futureSet = new HashSet<>();
    Set<UUID> futureAffectedStream = new HashSet<>();


    public void addFuture(Callable<Object> f, UUID uuid){
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

        TokenResponse response = this.builder.runtime.getStreamsView().appendAcquireToken( // a set of stream-IDs that contains the affected streams
                affectedStreams,
                null);

        long currentVersion = response.getTokenValue();

        setSnapshotTimestamp(currentVersion - 1);
        for (Callable<Object> f : futureSet) {
            try {
                f.call();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        address = this.builder.runtime.getStreamsView().appendWriteLogEntry(

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

    @Override
    public <R, T> R access(ICorfuSMRProxyInternal<T> proxy,
                           ICorfuSMRAccess<R, T> accessFunction,
                           Object[] conflictObject) {
        if (conflictObject[0] == CorfuTable.noConflict) {
            log.debug("Access[{},{}] in deferred manner. conflictObj={}", this, proxy, conflictObject);
            //just access the object and return. There is no real access to the underlying data structure -
            //this will happen later during the commit.
            //We need to access the object in order to create the deferred read object because we need the
            //scope variables, e.g., key
            return proxy.
                    getUnderlyingObject().
                    noAccess(o -> accessFunction.access(o));

            //R ret = accessFunction.access(V.object);
            //return ret;
        }
        return super.access(proxy, accessFunction, conflictObject);
    }
}
