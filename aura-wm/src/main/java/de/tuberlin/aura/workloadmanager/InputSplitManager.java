package de.tuberlin.aura.workloadmanager;

import de.tuberlin.aura.core.topology.Topology;
import org.apache.hadoop.mapred.InputSplit;

import java.io.Serializable;
import java.util.UUID;

public class InputSplitManager implements Serializable{

	/**
	 * The default input split assigner which is always used if a more specific assigner cannot be found.
	 */
	private final InputSplitAssigner defaultAssigner = new DefaultInputSplitAssigner();


	public InputSplitManager(){
	}


	/**
	 * Returns the next input split the input split manager (or the responsible {@link InputSplitAssigner} to be more
	 * precise) has chosen for the given executionNode to consume.
	 *
	 * @return the next input split to consume or <code>null</code> if the executionNode shall consume no more input splits
	 */
	public InputSplit getNextInputSplit(Topology.ExecutionNode executionNode, UUID topologyID, UUID taskID, final int sequenceNumber ) {
		InputSplit nextInputSplit = null;

		System.out.println("InputSplitManager::getNextInputSplit " + topologyID+", "+taskID);

		/*InputSplit nextInputSplit = this.inputSplitTracker.getInputSplitFromLog(vertex, sequenceNumber);
		if (nextInputSplit != null) {
			LOG.info("Input split " + nextInputSplit.getSplitNumber() + " for vertex " + vertex + " replayed from log");
			return nextInputSplit;
		}*/


		/*
		final ExecutionGroupVertex groupVertex = vertex.getGroupVertex();
		final InputSplitAssigner inputSplitAssigner = this.assignerCache.get(groupVertex);
		if (inputSplitAssigner == null) {
			final JobID jobID = groupVertex.getExecutionStage().getExecutionGraph().getJobID();
			LOG.error("Cannot find input assigner for group vertex " + groupVertex.getName() + " (job " + jobID + ")");
			return null;
		}
		*/

		// InputSplitAssigner knows about
		/*final InputSplitAssigner inputSplitAssigner = this.assignerCache.get(groupVertex);
		nextInputSplit = inputSplitAssigner.getNextInputSplit(vertex);

		if (nextInputSplit != null) {
			this.inputSplitTracker.addInputSplitToLog(vertex, sequenceNumber, nextInputSplit);
			LOG.info(vertex + " receives input split " + nextInputSplit.getSplitNumber());
		}*/	 

		return nextInputSplit;
	}

}
