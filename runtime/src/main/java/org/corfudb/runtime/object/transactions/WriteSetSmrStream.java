package org.corfudb.runtime.object.transactions;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.object.ISMRStream;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.Utils;

/**
 * Created by mwei on 3/13/17.
 *
 * <p>SMRStreamAdapter wraps an optimistic transaction execution context, per
 * object, with an SMRStream API.
 *
 * <p>The main purpose of wrapping the write-set of optimistic transactions as an
 * SMRStream is to provide the abstraction of a stream of SMREntries. The
 * SMRStream maintains for us a position in the sequence. We can consume it
 * in a forward direction, and scroll back to previously read entries.
 *
 * <p>First, forget about nested transactions for now, and neglect the contexts
 * stack; that is, assume the stack has size 1.
 *
 * <p>A reminder from AbstractTransactionalContext about the write-set of a
 * transaction:
 * * A write-set is a key component of a transaction.
 * * We collect the write-set as a map, organized by streams.
 * * For each stream, we record a pair:
 * *  - a set of conflict-parameters modified by this transaction on the
 * *  stream,
 * *  - a list of SMR updates by this transcation on the stream.
 * *
 *
 * <p>The implementation of the current() method looks at the write-set, picks
 * the list of SMRentries corresponding to the current object id, and returns
 * the entry in the list corredponding the the current SMRStream position.
 *
 * <p>previous() decrements the current SMRStream position and returns the entry
 * corresponding to it.
 *
 * <p>RemainingUpTo() returns a list of entries.
 *
 * <p>WriteSetSmrStream does not support the full API - neither append nor seek are
 * supported.
 *
 * <p>Enter nested transactions.
 *
 * <p>WriteSetSmrStream maintains the abstractions also across nested transactions.
 * It supports navigating forward/backward across the SMREntries in the entire transcation stack.
 *
 */
@Slf4j
@SuppressWarnings("checkstyle:abbreviationaswordinname")
public class WriteSetSmrStream implements ISMRStream {

    final WriteSetInfo writeSet;

    long pointer;

    // the specific stream-id for which this SMRstream wraps the write-set
    final UUID id;

    /**
     * Returns a new WriteSetSmrStream containing transactional contexts and stream id.
     * @param id  stream id
     */
    public WriteSetSmrStream(WriteSetInfo writeSet,
                             UUID id) {
        this.writeSet = writeSet;
        this.id = id;
        this.pointer = Address.NEVER_READ;
        reset();
    }

    /** Return whether stream current transaction is the thread current transaction.
     *
     * <p>This is validated by checking whether the current context
     * for this stream is the same as the current context for this thread.
     *
     * @return  True, if the stream current context is the thread current context.
     *          False otherwise.
     */
    public boolean isStreamCurrentContextThreadCurrentContext() {
        return writeSet
                .equals(TransactionalContext.getWriteSet());
    }

    /** Return whether we are the stream for this current thread
     *
     * <p>This is validated by checking whether the root context
     * for this stream is the same as the root context for this thread.
     *
     * @return  True, if the thread owns the optimistic stream
     *          False otherwise.
     */
    public boolean isStreamForThisThread() {
        return writeSet
                .equals(TransactionalContext.getWriteSet());
    }

    @Override
    public @Nonnull List<SMREntry> remainingUpTo(long maxGlobal) {
        if (Address.nonAddress(maxGlobal)) {
            return Collections.emptyList();
        }

        List<SMREntry> updateList = writeSet.getWriteSet()
                .getSMRUpdates(id);


        if (pointer >= updateList.size()) {
            return Collections.emptyList();
        }

        List<SMREntry> result = updateList.subList((int)pointer + 1,
                (int) Math.min(maxGlobal, updateList.size()));
        pointer = updateList.size() - 1;
        return result;
    }

    @Override
    public List<SMREntry> current() {

        List<SMREntry> updateList = writeSet.getWriteSet()
                .getSMRUpdates(id);

        if (updateList.size() == 0) {
            return null;
        }

        if (Address.nonAddress(pointer)) {
            return null;
        }

        return Collections
                .singletonList(updateList.get((int)pointer));
    }

    @Override
    public List<SMREntry> previous() {
        if (Address.nonAddress(pointer)) {
            throw new IllegalStateException("Attempt to rewind SMR stream past beginning");
        }

        pointer--;

        return current();
    }

    @Override
    public long pos() {
        return pointer;
    }

    @Override
    public void reset() {
        pointer = Address.NEVER_READ;
    }

    @Override
    public void seek(long globalAddress) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Stream<SMREntry> stream() {
        return streamUpTo(Address.MAX);
    }

    @Override
    public Stream<SMREntry> streamUpTo(long maxGlobal) {
        return remainingUpTo(maxGlobal)
                .stream();
    }

    @Override
    public long append(SMREntry entry,
                       Function<TokenResponse, Boolean> acquisitionCallback,
                       Function<TokenResponse, Boolean> deacquisitionCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UUID getID() {
        return id;
    }

    @Override
    public String toString() {
        return "WSSMRStream[" + Utils.toReadableId(getID()) + "]";
    }
}
