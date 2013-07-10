package org.apache.directmemory.buffer.spi;

public interface PartitionSliceSelector
{

    PartitionSlice selectPartitionSlice( Partition[] partitions );

    void freePartitionSlice( Partition partition, int partitionIndex, PartitionSlice slice );

}
