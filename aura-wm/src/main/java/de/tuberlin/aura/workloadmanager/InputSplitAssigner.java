package de.tuberlin.aura.workloadmanager;


import de.tuberlin.aura.core.topology.Topology;
import org.apache.hadoop.mapred.InputSplit;

public interface InputSplitAssigner {

	/**
	 * Registers an input vertex with the input split assigner.
	 * 
	 * @param node
	 *        the input vertex to register
	 */
	void registerNode( Topology.Node node );

	/**
	 * Unregisters an input vertex from the input split assigner. All resources allocated to the input vertex are freed
	 * as part of this operation.
	 * 
	 * @param node
	 *        the input vertex to unregister
	 */
	void unregisterNode( Topology.Node node );

	/**
	 * Returns the next input split that shall be consumed by the given input vertex.
	 * 
	 * @param node
	 *        the vertex for which the next input split to be consumed shall be determined
	 * @return the next input split to be consumed or <code>null</code> if no more splits shall be consumed by the given
	 *         vertex
	 */
	InputSplit getNextInputSplit( Topology.ExecutionNode node );
}
