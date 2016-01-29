package org.corfudb.runtime.view;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.exceptions.OverwriteException;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

/**
 * Created by mwei on 12/11/15.
 */
@Slf4j
public class StreamsView {

    /** The org.corfudb.runtime which backs this view. */
    CorfuRuntime runtime;

    public StreamsView(CorfuRuntime runtime)
    {
        this.runtime = runtime;
    }

    /** Get a view on a stream. The view has its own pointer to the stream.
     *
     * @param stream    The UUID of the stream to get a view on.
     * @return          A view
     */
    public StreamView get(UUID stream) {
        return new StreamView(runtime, stream);
    }

    public StreamView delete(UUID stream) {
        return null;
    }

    /** Write an object to multiple streams, retuning the physical address it
     * was written at.
     *
     * Note: While the completion of this operation guarantees that the write
     * has been persisted, it DOES NOT guarantee that the object has been
     * written to the stream. For example, another client may have deleted
     * the stream.
     *
     * @param object    The object to write to the stream.
     * @return          The address this
     */
    public long write(Set<UUID> streamIDs, Object object)
    {
        while (true) {
            SequencerClient.TokenResponse tokenResponse =
                    runtime.getSequencerView().nextToken(streamIDs, 1);
            long token = tokenResponse.getToken();
            log.trace("Write: acquired token = {}", token);
            try {
                runtime.getAddressSpaceView().write(token, streamIDs,
                        object, tokenResponse.getBackpointerMap());
                return token;
            } catch (OverwriteException oe)
            {
                log.debug("Overwrite occurred at {}, retrying.", token);
            }
        }
    }
}
