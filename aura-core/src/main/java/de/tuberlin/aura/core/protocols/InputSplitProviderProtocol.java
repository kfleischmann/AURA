package de.tuberlin.aura.core.protocols;

import org.apache.hadoop.mapred.InputSplit;
import java.util.UUID;

public interface InputSplitProviderProtocol extends ITM2WMProtocol {

	InputSplit requestNextInputSplit( final UUID topologyIDl, final UUID taskID, final int sequenceNumber );
}
