package de.tuberlin.aura.core.iosystem;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;

import de.tuberlin.aura.core.iosystem.queues.BufferQueue;

public class QueueManager<T> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final static Logger LOG = org.slf4j.LoggerFactory.getLogger(QueueManager.class);

    public static Map<UUID, QueueManager> BINDINGS = new HashMap<UUID, QueueManager>();

    private final Map<LongKey, BufferQueue<T>> inboundQueues;

    private final Map<LongKey, BufferQueue<T>> outboundQueues;

    private final BufferQueue.FACTORY<T> inboundFactory;

    private final BufferQueue.FACTORY<T> outboundFactory;

    private int inputQueuesCounter;

    private int outputQueuesCounter;

    private QueueManager(BufferQueue.FACTORY<T> inboundFactory, BufferQueue.FACTORY<T> outboundFactory) {

        this.inboundQueues = new HashMap<>();

        this.inboundFactory = inboundFactory;

        this.outboundQueues = new HashMap<>();

        this.outboundFactory = outboundFactory;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    /**
     * 
     * @param taskID
     * @param inboundFactory
     * @param outboundFactory
     * @param <F>
     * @return
     */
    public static <F> QueueManager<F> newInstance(UUID taskID, BufferQueue.FACTORY<F> inboundFactory, BufferQueue.FACTORY<F> outboundFactory) {
        QueueManager<F> instance = new QueueManager<>(inboundFactory, outboundFactory);
        BINDINGS.put(taskID, instance);
        return instance;
    }

    /**
     * [Christian] TODO: Synchronized necessary -> concurrent access from ConsumerEventHandler?
     * 
     * @param gateIndex
     * @return
     */
    public synchronized BufferQueue<T> getInboundQueue(int gateIndex, int channelIndex) {

        final LongKey key = new LongKey(gateIndex, channelIndex);
        if (inboundQueues.containsKey(key)) {
            return inboundQueues.get(key);
        }

        final BufferQueue<T> queue = inboundFactory.newInstance();
        inboundQueues.put(key, queue);
        ++this.inputQueuesCounter;

        return queue;
    }

    /**
     * [Christian] TODO: Synchronized -> concurrent access from ProducerEventHandler?
     * 
     * @param gateIndex
     * @param channelIndex
     * @return
     */
    public synchronized BufferQueue<T> getOutboundQueue(int gateIndex, int channelIndex) {

        final LongKey key = new LongKey(gateIndex, channelIndex);
        if (outboundQueues.containsKey(key)) {
            return outboundQueues.get(key);
        }

        final BufferQueue<T> queue = outboundFactory.newInstance();
        outboundQueues.put(key, queue);
        ++this.outputQueuesCounter;

        return queue;
    }

    public void clearInboundQueues() {
        inboundQueues.clear();
    }

    public void clearOutboundQueues() {
        outboundQueues.clear();
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    /**
     * We assume here that values for gate and channel do not exceed 16 bit (which is reasonable as
     * then u can have up to 65535 bufferQueues per gate and 65535 gates).
     */
    private static class LongKey {

        int gateIndex;

        int channelIndex;

        LongKey(int gateIndex, int channelIndex) {
            this.gateIndex = gateIndex;
            this.channelIndex = channelIndex;
        }

        @Override
        public int hashCode() {
            return gateIndex ^ (channelIndex >>> 16);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null)
                return false;
            if (!(obj instanceof LongKey))
                return false;
            if (hashCode() != obj.hashCode())
                return false;

            LongKey other = (LongKey) obj;
            return gateIndex == other.gateIndex && channelIndex == other.channelIndex;
        }
    }
}
