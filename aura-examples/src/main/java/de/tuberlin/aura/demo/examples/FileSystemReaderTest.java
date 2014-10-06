package de.tuberlin.aura.demo.examples;


import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.RowRecordReader;
import de.tuberlin.aura.core.record.RowRecordWriter;
import de.tuberlin.aura.core.record.tuples.AbstractTuple;
import de.tuberlin.aura.core.record.tuples.Tuple3;
import de.tuberlin.aura.core.task.spi.AbstractInvokeable;
import de.tuberlin.aura.core.task.spi.IDataConsumer;
import de.tuberlin.aura.core.task.spi.IDataProducer;
import de.tuberlin.aura.core.task.spi.ITaskDriver;
import de.tuberlin.aura.core.topology.Topology;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.SimpleLayout;
import org.slf4j.Logger;

import java.util.UUID;

public final class FileSystemReaderTest {

	// Disallow Instantiation.
	private FileSystemReaderTest() {}

	public static final class Record1 {

		public Record1() {}

		public int a;

		public int b;

		public int c;

		public String d;
	}

	/**
	 *
	 */
	public static class Source extends AbstractInvokeable {
		private Configuration conf;
		private final RowRecordWriter recordWriter;
		private LineReader in;
		private long pos;

		public Source(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
			super(taskDriver, producer, consumer, LOG);
			final Partitioner.IPartitioner partitioner = new Partitioner.HashPartitioner(new int[] {0});
			this.recordWriter = new RowRecordWriter(driver, AbstractTuple.class, 0, partitioner);
		}

		public void open() throws Throwable {

			recordWriter.begin();
			conf = new Configuration();

		}

		private void initialize( InputSplit split, Configuration conf ){

		}

		@Override
		public void run() throws Throwable {

			// request next input split from driver which forwards the request
			// to the workload manager
			InputSplit split = driver.getNextInputSplit();
			/*
			FileSplit fileSplit = (FileSplit)split;
			Path path = fileSplit.getPath();

			FileSystem fs = path.getFileSystem(conf);
			long start = fileSplit.getStart();
			long end = start + fileSplit.getLength();
			boolean skipFirstLine = false;

			FSDataInputStream filein = fs.open(fileSplit.getPath());
			if (start != 0){
				skipFirstLine = true;
				--start;
				filein.seek(start);
			}
			in = new LineReader(filein,conf);
			if(skipFirstLine){
				start += in.readLine(new Text(),0,(int)Math.min((long)Integer.MAX_VALUE, end - start));
			}
			this.pos = start;


			for (int i = 0; i < 10000; ++i) {
				recordWriter.writeObject(new Tuple3<>("Hans" + i, i, i));
			}*/
		}

		@Override
		public void close() throws Throwable {
			recordWriter.end();
			producer.done();
		}
	}

	/**
	 *
	 */
	public static class Sink extends AbstractInvokeable {

		private final RowRecordReader recordReader;

		public Sink(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {

			super(taskDriver, producer, consumer, LOG);

			this.recordReader = new RowRecordReader(taskDriver, 0);
		}

		@Override
		public void open() throws Throwable {
			consumer.openGate(0);
		}

		@Override
		public void run() throws Throwable {
			recordReader.begin();
			while (!recordReader.finished()) {
				final Tuple3<String, Integer, Integer> t = (Tuple3) recordReader.readObject();
				if (t != null) {
					double value0 = t._2;
					if (driver.getNodeDescriptor().taskIndex == 0) {
						System.out.println(driver.getNodeDescriptor().taskID + " value0:" + value0);
					}//if
				}//if
			}//while
			recordReader.end();
		}

		@Override
		public void close() throws Throwable {}
	}

	// ---------------------------------------------------
	// Main.
	// ---------------------------------------------------

	public static void main(String[] args) {

		final SimpleLayout layout = new SimpleLayout();
		new ConsoleAppender(layout);

		final LocalClusterSimulator lcs = new LocalClusterSimulator(IConfigFactory.load(IConfig.Type.SIMULATOR));
		final AuraClient ac = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT));
		final Topology.AuraTopologyBuilder atb1 = ac.createTopologyBuilder();

		//@formatter:off
		atb1.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source", 1, 1), Source.class, Record1.class )
				.connectTo("Sink", Topology.Edge.TransferType.ALL_TO_ALL)
				.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Sink", 1, 1), Sink.class);
		//@formatter:on

		final Topology.AuraTopology at1 = atb1.build("JOB 1");
		ac.submitTopology(at1, null);
		ac.awaitSubmissionResult(1);
		ac.closeSession();
		lcs.shutdown();
	}
}
