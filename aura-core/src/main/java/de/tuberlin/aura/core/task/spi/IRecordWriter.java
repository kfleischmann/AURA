package de.tuberlin.aura.core.task.spi;

import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.RowRecordModel;

/**
 *
 */
public interface IRecordWriter {

    public abstract void begin();

    public abstract void writeRecord(final RowRecordModel.Record record);

    public abstract void writeObject(final Object object);

    public abstract void end();

    public abstract void setPartitioner(final Partitioner.IPartitioner partitioner);
}
