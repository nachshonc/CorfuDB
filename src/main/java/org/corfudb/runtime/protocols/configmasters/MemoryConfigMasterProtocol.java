package org.corfudb.runtime.protocols.configmasters;

import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.gossip.IGossip;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.view.StreamData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by mwei on 4/30/15.
 */
public class MemoryConfigMasterProtocol implements IConfigMaster, IServerProtocol {

    private Logger log = LoggerFactory.getLogger(MemoryConfigMasterProtocol.class);
    private Map<String,String> options;
    private String host;
    private Integer port;
    private Long epoch;
    private ConcurrentMap<Long, byte[]> memoryArray;

    static ConcurrentHashMap<Integer, MemoryConfigMasterProtocol> memoryConfigMasters =
            new ConcurrentHashMap<Integer, MemoryConfigMasterProtocol>();

    public MemoryConfigMasterProtocol() {
        this("localhost", 0, new HashMap<String,String>(), 0L);
    }

    public static String getProtocolString()
    {
        return "mcm";
    }

    public static IServerProtocol protocolFactory(String host, Integer port, Map<String,String> options, Long epoch)
    {
        IServerProtocol res;
        if ((res = memoryConfigMasters.get(port)) != null)
        {
            return res;
        }
        return new MemoryConfigMasterProtocol(host, port, options, epoch);
    }

    public MemoryConfigMasterProtocol(String host, Integer port, Map<String,String> options, Long epoch)
    {
        this.host = host;
        this.port = port;
        this.options = options;
        this.epoch = epoch;
    }

    /**
     * Adds a new stream to the system.
     *
     * @param logID    The ID of the log the stream starts on.
     * @param streamID The streamID of the stream.
     * @param startPos The start position of the stream.
     * @return True if the stream was successfully added to the system, false otherwise.
     */
    @Override
    public boolean addStream(UUID logID, UUID streamID, long startPos) {
        return false;
    }

    /**
     * Adds a new stream to the system.
     *
     * @param logID    The ID of the log the stream starts on.
     * @param streamID The streamID of the stream.
     * @param startPos The start position of the stream.
     * @param nopass
     * @return True if the stream was successfully added to the system, false otherwise.
     */
    @Override
    public boolean addStreamCM(UUID logID, UUID streamID, long startPos, boolean nopass) {
        return false;
    }

    /**
     * Gets information about a stream in the system.
     *
     * @param streamID The ID of the stream to retrieve.
     * @return A StreamData object containing information about the stream, or null if the
     * stream could not be found.
     */
    @Override
    public StreamData getStream(UUID streamID) {
        return null;
    }

    /**
     * Adds a log to the system.
     *
     * @param logID The ID of the log to add.
     * @param path  True, if the log was added to the system, or false otherwise.
     */
    @Override
    public boolean addLog(UUID logID, String path) {
        return false;
    }

    /**
     * Gets information about all logs known to the system.
     *
     * @return A map containing all logs known to the system.
     */
    @Override
    public Map<UUID, String> getAllLogs() {
        return null;
    }

    /**
     * Gets the configuration string for a log, given its id.
     *
     * @param logID The ID of the log to retrieve.
     * @return The configuration string used to access that log.
     */
    @Override
    public String getLog(UUID logID) {
        return null;
    }

    /**
     * Sends gossip to this configuration master. Unreliable.
     *
     * @param gossip The gossip object to send to the remote configuration master.
     */
    @Override
    public void sendGossip(IGossip gossip) {

    }

    /**
     * Resets the entire log, and increments the epoch. Use only during testing to restore the system to a
     * known state.
     */
    @Override
    public void resetAll() {

    }

    /**
     * Returns the full server string.
     *
     * @return A server string.
     */
    @Override
    public String getFullString() {
        return "mcm";
    }

    /**
     * Returns the host
     *
     * @return The hostname for the server.
     */
    @Override
    public String getHost() {
        return host;
    }

    /**
     * Returns the port
     *
     * @return The port number of the server.
     */
    @Override
    public Integer getPort() {
        return port;
    }

    /**
     * Returns the option map
     *
     * @return The option map for the server.
     */
    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    /**
     * Returns a boolean indicating whether or not the server was reachable.
     *
     * @return True if the server was reachable, false otherwise.
     */
    @Override
    public boolean ping() {
        return true;
    }

    /**
     * Sets the epoch of the server. Used by the configuration master to switch epochs.
     *
     * @param epoch
     */
    @Override
    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    /**
     * Resets the server. Used by the configuration master to reset the state of the server.
     * Should eliminate ALL hard state!
     *
     * @param epoch
     */
    @Override
    public void reset(long epoch) throws NetworkException {

    }
}
