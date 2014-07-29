package de.tuberlin.aura.core.protocols;

import org.apache.hadoop.mapred.InputSplit;
import java.util.UUID;

public interface InputSplitProviderProtocol {

	InputSplit requestNextInputSplit( final UUID topologyID, final UUID taskID, final int sequenceNumber );
}
