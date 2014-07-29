package de.tuberlin.aura.taskmanager;

import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.protocols.InputSplitProviderProtocol;
import org.apache.hadoop.mapred.InputSplit;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskInputSplitProvider implements Serializable {
	private final Descriptors.AbstractNodeDescriptor nodeDescriptor;

	private final InputSplitProviderProtocol globalInputSplitProvider;

	private final AtomicInteger sequenceNumber = new AtomicInteger(0);

	public TaskInputSplitProvider( Descriptors.AbstractNodeDescriptor nodeDescriptor, final InputSplitProviderProtocol globalInputSplitProvider ){
		this.nodeDescriptor = nodeDescriptor;
		this.globalInputSplitProvider = globalInputSplitProvider;
	}

	public InputSplit getNextInputSplit() {
		synchronized (this.globalInputSplitProvider) {
			return this.globalInputSplitProvider.requestNextInputSplit(nodeDescriptor.topologyID, nodeDescriptor.taskID, this.sequenceNumber.getAndIncrement() );
		}
	}
}