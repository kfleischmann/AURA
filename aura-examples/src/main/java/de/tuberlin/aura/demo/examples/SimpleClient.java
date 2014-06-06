package de.tuberlin.aura.demo.examples;

public final class SimpleClient {

//    /**
//     * Logger.
//     */
//    private static final Logger LOG = LoggerFactory.getLogger(SimpleClient.class);
//
//    // Disallow Instantiation.
//    private SimpleClient() {}
//
//    /**
//     *
//     */
//    public static class TaskMap1 extends AbstractInvokeable {
//
//        private final IRecordWriter recordWriter;
//
//        public TaskMap1(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
//
//            super(taskDriver, producer, consumer, LOG);
//
//            this.recordWriter = new TaskRecordWriter(BufferAllocator._64K, producer);
//        }
//
//        @Override
//        public void run() throws Throwable {
//
//            final UUID srcTaskID = driver.getNodeDescriptor().taskID;
//
//            final List<Descriptors.AbstractNodeDescriptor> outputBinding = driver.getBindingDescriptor().outputGateBindings.get(0);
//
//            int i = 0;
//            while (i++ < 10 && isInvokeableRunning()) {
//
//                for (int index = 0; index < outputBinding.size(); ++index) {
//
//                    final UUID dstTaskID = getTaskID(0, index);
//
//                    final MemoryView outBuffer = producer.alloc();
//
//                    recordWriter.selectBuffer(outBuffer);
//
//                    recordWriter.writeRecord(new Integer(100));
//
//                    final IOEvents.TransferBufferEvent outEvent = new IOEvents.TransferBufferEvent(srcTaskID, dstTaskID, outBuffer);
//
//                    producer.emit(0, index, outEvent);
//                }
//            }
//        }
//
//        @Override
//        public void close() throws Throwable {
//            producer.done();
//        }
//    }
//
//    /**
//     *
//     */
//    public static class TaskLeftInput extends AbstractInvokeable {
//
//        private final IRecordWriter recordWriter;
//
//        private final IRecordReader recordReader;
//
//        public TaskLeftInput(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
//
//            super(taskDriver, producer, consumer, LOG);
//
//            this.recordWriter = new TaskRecordWriter(BufferAllocator._64K, producer);
//
//            this.recordReader = new TaskRecordReader(BufferAllocator._64K);
//        }
//
//        @Override
//        public void open() throws Throwable {
//            consumer.openGate(0);
//        }
//
//        @Override
//        public void run() throws Throwable {
//
//            while (!consumer.isExhausted() && isInvokeableRunning()) {
//
//                final IOEvents.TransferBufferEvent inEvent = consumer.absorb(0);
//
//                if (inEvent != null) {
//
//                    recordReader.selectBuffer(inEvent.buffer);
//
//                    final Integer value = recordReader.readRecord(Integer.class);
//
//                    inEvent.buffer.free();
//
//                    final MemoryView outBuffer = producer.alloc();
//
//                    recordWriter.selectBuffer(outBuffer);
//
//                    recordWriter.writeRecord(new Integer(value + 100));
//
//                    producer.store(outBuffer);
//                }
//            }
//        }
//    }
//
//    /**
//     *
//     */
//    public static class TaskRightInput extends AbstractInvokeable {
//
//        private final IRecordWriter recordWriter;
//
//        public TaskRightInput(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
//
//            super(taskDriver, producer, consumer, LOG);
//
//            this.recordWriter = new TaskRecordWriter(BufferAllocator._64K, producer);
//        }
//
//        @Override
//        public void run() throws Throwable {
//
//            final UUID srcTaskID = driver.getNodeDescriptor().taskID;
//
//            final List<Descriptors.AbstractNodeDescriptor> outputBinding = driver.getBindingDescriptor().outputGateBindings.get(0);
//
//            int i = 0;
//            while (i++ < 10 && isInvokeableRunning()) {
//
//                for (int index = 0; index < outputBinding.size(); ++index) {
//
//                    final UUID dstTaskID = getTaskID(0, index);
//
//                    final MemoryView outBuffer = producer.alloc();
//
//                    recordWriter.selectBuffer(outBuffer);
//
//                    recordWriter.writeRecord(new Integer(50));
//
//                    final IOEvents.TransferBufferEvent outEvent = new IOEvents.TransferBufferEvent(srcTaskID, dstTaskID, outBuffer);
//
//                    producer.emit(0, index, outEvent);
//                }
//            }
//        }
//
//        @Override
//        public void close() throws Throwable {
//            producer.done();
//        }
//    }
//
//    /**
//     *
//     */
//    public static class TaskBinaryInput extends AbstractInvokeable {
//
//        private final IRecordWriter recordWriter;
//
//        private final IRecordReader recordReaderLeft;
//
//        private final IRecordReader recordReaderRight;
//
//        public TaskBinaryInput(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
//
//            super(taskDriver, producer, consumer, LOG);
//
//            this.recordWriter = new TaskRecordWriter(BufferAllocator._64K, producer);
//
//            this.recordReaderLeft = new TaskRecordReader(BufferAllocator._64K);
//
//            this.recordReaderRight = new TaskRecordReader(BufferAllocator._64K);
//        }
//
//        @Override
//        public void open() throws Throwable {
//            consumer.openGate(0);
//            consumer.openGate(1);
//        }
//
//        @Override
//        public void run() throws Throwable {
//
//            final UUID srcTaskID = driver.getNodeDescriptor().taskID;
//
//            final List<Descriptors.AbstractNodeDescriptor> outputBinding = driver.getBindingDescriptor().outputGateBindings.get(0);
//
//            while (!consumer.isExhausted() && isInvokeableRunning()) {
//
//                final IOEvents.TransferBufferEvent inBufferLeft = consumer.absorb(0);
//
//                final IOEvents.TransferBufferEvent inBufferRight = consumer.absorb(1);
//
//
//                Integer valueLeft = 0;
//
//                if (inBufferLeft != null) {
//
//                    recordReaderLeft.selectBuffer(inBufferLeft.buffer);
//
//                    valueLeft = recordReaderLeft.readRecord(Integer.class);
//
//                    inBufferLeft.buffer.free();
//                }
//
//                Integer valueRight = 0;
//
//                if (inBufferRight != null) {
//
//                    recordReaderRight.selectBuffer(inBufferRight.buffer);
//
//                    valueRight = recordReaderRight.readRecord(Integer.class);
//
//                    inBufferRight.buffer.free();
//                }
//
//
//                if (inBufferLeft != null && inBufferRight != null) {
//
//                    for (int index = 0; index < outputBinding.size(); ++index) {
//
//                        final UUID dstTaskID = getTaskID(0, index);
//
//                        final MemoryView outBuffer = producer.alloc();
//
//                        recordWriter.selectBuffer(outBuffer);
//
//                        recordWriter.writeRecord(new Integer(valueLeft + valueRight));
//
//                        final IOEvents.TransferBufferEvent outEvent = new IOEvents.TransferBufferEvent(srcTaskID, dstTaskID, outBuffer);
//
//                        producer.emit(0, index, outEvent);
//                    }
//                }
//            }
//        }
//
//        @Override
//        public void close() throws Throwable {
//            producer.done();
//        }
//    }
//
//    /**
//     *
//     */
//    public static class TaskMap3 extends AbstractInvokeable {
//
//        private IRecordReader recordReader;
//
//        public TaskMap3(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
//
//            super(taskDriver, producer, consumer, LOG);
//
//            this.recordReader = new TaskRecordReader(BufferAllocator._64K);
//        }
//
//        @Override
//        public void open() throws Throwable {
//            consumer.openGate(0);
//        }
//
//        @Override
//        public void run() throws Throwable {
//
//            while (!consumer.isExhausted() && isInvokeableRunning()) {
//
//                final IOEvents.TransferBufferEvent inEvent = consumer.absorb(0);
//
//                if (inEvent != null) {
//
//                    recordReader.selectBuffer(inEvent.buffer);
//
//                    final Integer value = recordReader.readRecord(Integer.class);
//
//                    LOG.info("----------------> READ BUFFER: " + value);
//
//                    inEvent.buffer.free();
//                }
//            }
//        }
//    }
//
//    // ---------------------------------------------------
//    // Main.
//    // ---------------------------------------------------
//
//    public static void main(String[] args) {
//
//        final SimpleLayout layout = new SimpleLayout();
//        final ConsoleAppender consoleAppender = new ConsoleAppender(layout);
//
//        // Local
//        final String measurementPath = "/home/tobias/Desktop/logs";
//        final String zookeeperAddress = "localhost:2181";
//        final LocalClusterSimulator lce = new LocalClusterSimulator(LocalClusterSimulator.ExecutionMode.EXECUTION_MODE_SINGLE_PROCESS, true, zookeeperAddress, 4, measurementPath);
//
//        // Wally
//        //final String zookeeperAddress = "wally101.cit.tu-berlin.de:2181";
//
//        final AuraClient ac = new AuraClient(zookeeperAddress, 10000, 11111);
//
//        final AuraGraph.AuraTopologyBuilder atb1 = ac.createTopologyBuilder();
//
//        atb1.addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "TaskMap1", 2, 1), TaskMap1.class).
//                connectTo("LeftStorage", AuraGraph.Edge.TransferType.POINT_TO_POINT)
//            .addStorageNode(new AuraGraph.StorageNode(UUID.randomUUID(), "LeftStorage", 2, 1));
//            //.addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "TaskLeftInput", 2, 1), TaskLeftInput.class);
//
//        final AuraGraph.AuraTopology at1 = atb1.build("JOB 1");
//
//        ac.submitTopology(at1, null);
//
//        try {
//            new BufferedReader(new InputStreamReader(System.in)).readLine();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        final AuraGraph.AuraTopologyBuilder atb2 = ac.createTopologyBuilder();
//
//        atb2.addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "TaskRightInput", 2, 1), TaskRightInput.class)
//                .connectTo("TaskBinaryInput", AuraGraph.Edge.TransferType.ALL_TO_ALL)
//            .connectFrom("LeftStorage", "TaskBinaryInput", AuraGraph.Edge.TransferType.ALL_TO_ALL)
//            .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "TaskBinaryInput", 2, 1), TaskBinaryInput.class)
//                .connectTo("TaskMap3", AuraGraph.Edge.TransferType.ALL_TO_ALL)
//            .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "TaskMap3", 2, 1), TaskMap3.class);
//
//        final AuraGraph.AuraTopology at2 = atb2.build("JOB 2");
//
//        ac.submitToTopology(at1.topologyID, at2);
//
//        try {
//            new BufferedReader(new InputStreamReader(System.in)).readLine();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        ac.closeSession();
//
//        //atb1.addNode(new AuraGraph.Node(taskNodeID, "Task1", 4, 1), TaskMap1.class);
//        //.connectTo("Task3", AuraGraph.Edge.TransferType.ALL_TO_ALL)
//        //.addNode(new AuraGraph.Node(UUID.randomUUID(), "Task2", 4, 1), TaskLeftInput.class)
//        //.connectTo("Task3", AuraGraph.Edge.TransferType.ALL_TO_ALL)
//        //.addNode(new AuraGraph.Node(UUID.randomUUID(), "Task3", 4, 1), TaskBinaryInput.class)
//        //.connectTo("Task4", AuraGraph.Edge.TransferType.ALL_TO_ALL)
//        //.addNode(new AuraGraph.Node(UUID.randomUUID(), "Task4", 4, 1), TaskMap3.class);
//
//
//        // lce.shutdown();
//    }
}
