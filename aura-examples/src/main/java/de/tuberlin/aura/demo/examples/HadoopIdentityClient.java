package de.tuberlin.aura.demo.examples;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.BufferAllocator;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.task.spi.AbstractInvokeable;
import de.tuberlin.aura.core.task.spi.IDataConsumer;
import de.tuberlin.aura.core.task.spi.IDataProducer;
import de.tuberlin.aura.core.task.spi.ITaskDriver;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;


import org.apache.hadoop.conf.Configuration;

public class HadoopIdentityClient {




    public static class HdfsSink extends AbstractInvokeable {


        long count = 0;

		public HdfsSink(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
            super(context, producer, consumer, LOG);
        }

        FileSystem fs = null;
        FSDataOutputStream out = null;
        FileStatus fileStatus = null;

        @Override
        public void open() throws Throwable {
            consumer.openGate(0);


            Configuration conf = new Configuration();
            conf.addResource(new Path("/home/kay/hadoop1/conf/core-site.xml"));

            final String output = "hdfs:///normalized_full.txt.copy.aura";
            Path outFile = new Path(output);

            fs = FileSystem.get(conf);

            out = fs.create(outFile);

            fileStatus = fs.getFileStatus(outFile);
            System.out.println("len:"+fileStatus.getLen());

        }

        @Override
        public void close() throws Throwable {
            out.close();
        }


        @Override
        public void run() throws Throwable {

            int totalReceived=0;
            int pos=0;
            while (!consumer.isExhausted() && isInvokeableRunning()) {
                final IOEvents.TransferBufferEvent event = consumer.absorb(0);


                if (event != null) {
                    count++;

                    System.out.println("RECEIVED: "+totalReceived );

                    totalReceived+=event.buffer.memory.length;
                    System.out.println( "RECEUVED-size:"+event.buffer.memory.length );

                    out.write(event.buffer.memory);

                    //out.write(event.buffer.memory, 0, event.buffer.memory. );

                    pos+=event.buffer.memory.length;

                    // if (count % 10000 == 0)
                    // LOG.info("Sink receive {}.", count);
                    // LOG.info("free in sink");

                    // HDFS: Write

                    event.buffer.free();
                }
            }

            LOG.info("Sink finished {}.", count);
        }
    }

    public static class IdentityNode extends TaskInvokeable {

        public IdentityNode(TaskDriverContext driverContext, DataProducer producer, DataConsumer consumer, Logger LOG) {
            super(driverContext, producer, consumer, LOG);
        }

        @Override
        public void run() throws Throwable {
            final UUID taskID = driverContext.taskDescriptor.taskID;

            int index=0;
            while (!consumer.isExhausted() && isInvokeableRunning()) {
                final IOEvents.TransferBufferEvent event = consumer.absorb(0);

                if (event != null) {
                    index++;

                    // forward tuple
                    // index??
                    producer.emit(0, index, event);

                    event.buffer.free();
                }
            }

        }
    }


    public static class HdfsSource extends TaskInvokeable {

        // List<InputSplits>

        // nextSplit() -> seek()


        public HdfsSource(final TaskDriverContext context, DataProducer producer, final DataConsumer consumer, final Logger LOG) {
            super(context, producer, consumer, LOG);
        }

        public boolean notFinished(){
            return true;
        }

        int RECORDS = 100;

        @Override
        public void run() throws Throwable {
            final UUID taskID = driverContext.taskDescriptor.taskID;

            int i=0;
            int pos=0;
            int len= BufferAllocator._64K;
            int total_read=0;
            while ( isInvokeableRunning()) {
                final List<Descriptors.TaskDescriptor> outputs = driverContext.taskBindingDescriptor.outputGateBindings.get(0);
                final MemoryView buffer = producer.allocBlocking();

                // fill current buffer
                int bytesRead = readIntoBuffer(buffer.memory, pos, len );

                total_read += bytesRead;

                System.out.println("Buffer: "+buffer.memory.length);
                System.out.println("RUN: "+i+", total-read: "+total_read);


                if(bytesRead>0) {
                    // forward the buffer to each output
                    for (int index = 0; index < outputs.size(); ++index) {
                        final UUID outputTaskID = getTaskID(0, index);

                        buffer.retain();
                        final IOEvents.TransferBufferEvent event = new IOEvents.TransferBufferEvent(taskID, outputTaskID, buffer);

                        producer.emit(0, index, event);
                    }

                    pos += len;
                } else {
                    break;
                }

                i++;

                // free??

            }

            // read from hadoop Sink
            LOG.error("Hadoop finished");
        }

        @Override
        public void open() throws IOException {
            Configuration conf = new Configuration();
            conf.addResource(new Path("/home/kay/hadoop1/conf/core-site.xml"));


            // Local#
            final String input = "hdfs:///normalized_full.txt";
            Path inFile = new Path(input);

            fs = FileSystem.get(conf);

            in = fs.open(inFile);

            fileStatus = fs.getFileStatus(inFile);
            System.out.println("len:"+fileStatus.getLen());
        }

        FileSystem fs = null;
        FSDataInputStream in = null;
        FileStatus fileStatus = null;

        public int readIntoBuffer( byte buffer[], int off, int len ){

            try {
                //System.out.println("read: "+off+", len:"+len);
                int bytesRead = in.read(buffer); //off, len );
                return bytesRead;

            } catch (IOException e) {
                System.out.println("Error while copying file");
            } finally {
            }
            return 0;
        }

        @Override
        public void close() throws Throwable {
            in.close();

            LOG.debug("{} {} done", driverContext.taskDescriptor.name, driverContext.taskDescriptor.taskIndex);
            producer.done();
        }
    }

    public static void executeHdfsOnAura (String[] args){
        int machines = 2;
        int cores = 4;
        int runs = 1;


        // Local#
        //final String input = args[0];  //"hdfs://hadoop1/input_file";
        //final String output = args[1]; //"hdfs://hadoop1/output_file";
        final String input = "hdfs:///normalized_full.txt";
        final String output = "hdfs:///normalized_full.txt.copy3";


        final String measurementPath = "/home/kay/local_measurements";
        final String zookeeperAddress = "localhost:2181";
        final LocalClusterSimulator lce =
                new LocalClusterSimulator(LocalClusterSimulator.ExecutionMode.EXECUTION_MODE_SINGLE_PROCESS,
                        true,
                        zookeeperAddress,
                        machines,
                        measurementPath);


        final AuraClient ac = new AuraClient(zookeeperAddress, 10000, 11111);
        AuraDirectedGraph.AuraTopology topology = buildTopology(ac, machines, cores);

        ac.submitTopology( topology, null);
    }


    public static AuraDirectedGraph.AuraTopology buildTopology(AuraClient client, int machines, int tasksPerMaschine){
        int executionUnits = machines * tasksPerMaschine;
        AuraDirectedGraph.AuraTopologyBuilder atb;

        // // 2 layered - point2point connection
        atb = client.createTopologyBuilder();
        atb.addNode(new AuraDirectedGraph.Node(UUID.randomUUID(), "hdfs", 1, 1), HdfsSource.class)

                //.connectTo("hdfs", AuraDirectedGraph.Edge.TransferType.POINT_TO_POINT)
                //.addNode(new AuraDirectedGraph.Node(UUID.randomUUID(), "identity", 1, 1), IdentityNode.class)

                .connectTo("Sink", AuraDirectedGraph.Edge.TransferType.POINT_TO_POINT)
                .addNode(new AuraDirectedGraph.Node(UUID.randomUUID(), "Sink", 1, 1), HdfsSink.class);


        return atb.build("Job: Hdfs reader", EnumSet.of(AuraDirectedGraph.AuraTopology.MonitoringType.NO_MONITORING)) ;
    }


    // ---------------------------------------------------
    // Main.
    // ---------------------------------------------------
    static void printAndExit(String str) {
        System.err.println(str);
        System.exit(1);
    }

    public static void main(String[] args) throws IOException {

        executeHdfsOnAura(args);


    }

    public static void test_Hdfs(String[] argd ) throws IOException  {

        Configuration conf = new Configuration();
        conf.addResource(new Path("/home/kay/hadoop1/conf/core-site.xml"));


        // Local#
        final String input = "hdfs:///normalized_full.txt";
        final String output = "hdfs:///normalized_full.txt.copy3";

        System.out.println( conf.getRaw("fs.default.name") );


        Path inFile = new Path(input);
        Path outFile = new Path(output);


        FileSystem fs = FileSystem.get(conf);
        // Check if input/output are valid
        if (!fs.exists(inFile))
            printAndExit("Input file not found");
        if (!fs.isFile(inFile))
            printAndExit("Input should be a file");
        //if (fs.exists(outFile))
        //    printAndExit("Output already exists");

        FileStatus fileStatus = fs.getFileStatus(inFile);
        final org.apache.hadoop.fs.BlockLocation[] blkLocations = fs.getFileBlockLocations( fileStatus, 0, 243698823 );


        System.out.println("block-size:"+fileStatus.getBlockSize() );
        System.out.println("block-len :"+fileStatus.getLen() );

        System.out.println("blocks: "+blkLocations.length );



        // Read from and write to new file
        FSDataInputStream in = fs.open(inFile);
        FSDataOutputStream out = fs.create(outFile);
        byte buffer[] = new byte[256];
        try {
            int bytesRead = 0;
            while ((bytesRead = in.read(buffer)) > 0) {
                System.out.println( new String(buffer));
                out.write(buffer, 0, bytesRead);
            }
        } catch (IOException e) {
            System.out.println("Error while copying file");
        } finally {
            in.close();
            out.close();
        }

    }

}
