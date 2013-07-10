package org.apache.directmemory.buffer.spi;

public interface PartitionFactory
{

    Partition newPartition( int partitionIndex, int sliceByteSize, int slices,
                            PartitionSliceSelector partitionSliceSelector );

}
