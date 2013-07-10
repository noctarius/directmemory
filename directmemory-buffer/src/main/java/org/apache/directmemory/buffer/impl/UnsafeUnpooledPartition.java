package org.apache.directmemory.buffer.impl;

import org.apache.directmemory.buffer.spi.Partition;
import org.apache.directmemory.buffer.spi.PartitionFactory;
import org.apache.directmemory.buffer.spi.PartitionSliceSelector;

public class UnsafeUnpooledPartition
    extends AbstractUnpooledPartition
{

    public static final PartitionFactory UNSAFE_PARTITION_FACTORY = new PartitionFactory()
    {

        @Override
        public Partition newPartition( int partitionIndex, int sliceByteSize, int slices,
                                       PartitionSliceSelector partitionSliceSelector )
        {
            return new UnsafeUnpooledPartition( partitionIndex, slices, sliceByteSize, partitionSliceSelector );
        }
    };

    private UnsafeUnpooledPartition( int partitionIndex, int slices, int sliceByteSize,
                                     PartitionSliceSelector partitionSliceSelector )
    {
        super( partitionIndex, slices, sliceByteSize, partitionSliceSelector, true );
    }

    @Override
    protected AbstractPartitionSlice createPartitionSlice( int index, int sliceByteSize )
    {
        return new UnsafePartitionSlice( index, this, sliceByteSize );
    }

}
