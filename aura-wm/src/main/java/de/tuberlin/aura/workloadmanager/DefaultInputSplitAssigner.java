package de.tuberlin.aura.workloadmanager;

import de.tuberlin.aura.core.topology.Topology;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

/**
 * This is the default implementation of the {@link InputSplitAssigner} interface. The default input split assigner
 * simply returns all input splits of an input vertex in the order they were originally computed. The default input
 * split assigner is always used when a more specific {@link InputSplitAssigner} could not be found.
 * <p>
 * This class is thread-safe.
 * 
 */
public class DefaultInputSplitAssigner implements InputSplitAssigner {

	private static final Logger LOG = Logger.getLogger(DefaultInputSplitAssigner.class);

	/**
	 * The split map stores a list of all input splits that still must be consumed by a specific input vertex.
	 */
	private final ConcurrentMap<Topology.Node, Queue<InputSplit>> splitMap = new ConcurrentHashMap<Topology.Node, Queue<InputSplit>>();


	@Override
	public void registerNode(final Topology.Node node) {

		final InputSplit[] inputSplits = node.getInputSplits();

		if (inputSplits == null) {
			LOG.info("");
			return;
		}

		if (inputSplits.length == 0) {
			return;
		}

		final Queue<InputSplit> queue = new ConcurrentLinkedQueue<InputSplit>();
		if (this.splitMap.putIfAbsent( node , queue) != null) {
			LOG.error("Node " + node + " already has a split queue");
		}

		queue.addAll(Arrays.asList(inputSplits));
	}


	@Override
	public void unregisterNode(final Topology.Node node) {

		this.splitMap.remove(node);
	}


	@Override
	public InputSplit getNextInputSplit(final Topology.ExecutionNode executionNode) {

		System.out.println("DefaultInputSplitAssinger::getNextInputSplit");
		// get input splits from logical node

		final Queue<InputSplit> queue = this.splitMap.get(executionNode.logicalNode);

		if (queue == null) {
			System.out.println("no input split found");
			//final JobID jobID = executionNode.getExecutionGraph().getJobID();
			//LOG.error("Cannot find split queue for vertex " + vertex.getGroupVertex() + " (job " + jobID + ")");
			return null;
		}

		InputSplit nextSplit = queue.poll();

		if (LOG.isDebugEnabled() && nextSplit != null) {
			LOG.debug("Assigning split " + nextSplit + " to " + executionNode.getNodeDescriptor().name );
		}

		return nextSplit;
	}
}
