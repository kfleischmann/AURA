package de.tuberlin.aura.client.api;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.descriptors.DescriptorFactory;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.IOEvents.ControlEventType;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.protocols.ClientWMProtocol;
import de.tuberlin.aura.core.task.usercode.UserCodeExtractor;
import de.tuberlin.aura.core.topology.AuraGraph.AuraTopology;
import de.tuberlin.aura.core.topology.AuraGraph.AuraTopologyBuilder;
import de.tuberlin.aura.core.zookeeper.ZookeeperConnectionWatcher;
import de.tuberlin.aura.core.zookeeper.ZookeeperHelper;

public final class AuraClient {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(AuraClient.class);

    public final IOManager ioManager;

    public final RPCManager rpcManager;

    public final ClientWMProtocol clientProtocol;

    public final UserCodeExtractor codeExtractor;

    public final Map<UUID, EventHandler> registeredTopologyMonitors;

    public final IORedispatcher ioHandler;

    public final UUID clientSessionID;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    /**
     * @param zkServer
     * @param controlPort
     * @param dataPort
     */
    public AuraClient(final String zkServer, int controlPort, int dataPort) {
        // sanity check.
        if (zkServer == null)
            throw new IllegalArgumentException("zkServer == null");
        if (dataPort < 1024 || dataPort > 65535)
            throw new IllegalArgumentException("dataPort invalid");
        if (controlPort < 1024 || controlPort > 65535)
            throw new IllegalArgumentException("controlPort invalid port number");

        final MachineDescriptor md = DescriptorFactory.createMachineDescriptor(dataPort, controlPort);

        this.ioManager = new IOManager(md, null);

        this.rpcManager = new RPCManager(ioManager);

        this.codeExtractor = new UserCodeExtractor(false);

        this.codeExtractor.addStandardDependency("java")
                          .addStandardDependency("org/apache/log4j")
                          .addStandardDependency("io/netty")
                          .addStandardDependency("de/tuberlin/aura/core");

        final ZooKeeper zookeeper;

        final MachineDescriptor wmMachineDescriptor;
        try {
            zookeeper = new ZooKeeper(zkServer, ZookeeperHelper.ZOOKEEPER_TIMEOUT, new ZookeeperConnectionWatcher(new IEventHandler() {

                @Override
                public void handleEvent(Event event) {}
            }));

            wmMachineDescriptor = (MachineDescriptor) ZookeeperHelper.readFromZookeeper(zookeeper, ZookeeperHelper.ZOOKEEPER_WORKLOADMANAGER);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        ioHandler = new IORedispatcher();

        ioManager.addEventListener(ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED, ioHandler);

        ioManager.addEventListener(ControlEventType.CONTROL_EVENT_TOPOLOGY_FAILURE, ioHandler);

        ioManager.connectMessageChannelBlocking(wmMachineDescriptor);
        clientProtocol = rpcManager.getRPCProtocolProxy(ClientWMProtocol.class, wmMachineDescriptor);

        this.registeredTopologyMonitors = new HashMap<>();

        // create examples session.
        this.clientSessionID = UUID.randomUUID();

        clientProtocol.openSession(clientSessionID);
        LOG.info("CLIENT IS READY");
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    /**
     * @return
     */
    public AuraTopologyBuilder createTopologyBuilder() {
        return new AuraTopologyBuilder(ioManager.machine.uid, codeExtractor);
    }

    /**
     * @param topology
     * @param handler
     */
    public void submitTopology(final AuraTopology topology, final EventHandler handler) {
        // sanity check.
        if (topology == null)
            throw new IllegalArgumentException("topology == null");

        if (handler != null) {
            registeredTopologyMonitors.put(topology.topologyID, handler);
        }
        clientProtocol.submitTopology(clientSessionID, topology);
    }

    /**
     *
     */
    public void closeSession() {
        clientProtocol.closeSession(clientSessionID);
    }

    /**
     *
     * @param topologyID
     * @param topology
     */
    public void submitToTopology(final UUID topologyID, final AuraTopology topology) {
        // sanity check.
        if (topologyID == null)
            throw new IllegalArgumentException("topologyID == null");
        if (topology == null)
            throw new IllegalArgumentException("topology == null");

        clientProtocol.submitToTopology(clientSessionID, topologyID, topology);
    }


    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private final class IORedispatcher extends EventHandler {

        @Handle(event = IOEvents.ControlIOEvent.class, type = ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED)
        private void handleTopologyFinished(final IOEvents.ControlIOEvent event) {
            LOG.info("Topology finished.");
        }

        @Handle(event = IOEvents.ControlIOEvent.class, type = ControlEventType.CONTROL_EVENT_TOPOLOGY_FAILURE)
        private void handleTopologyFailure(final IOEvents.ControlIOEvent event) {
            LOG.info("Topology failed.");
        }
    }
}
