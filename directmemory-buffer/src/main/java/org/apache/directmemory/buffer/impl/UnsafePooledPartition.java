package org.apache.directmemory.buffer.impl;

import org.apache.directmemory.buffer.spi.Partition;
import org.apache.directmemory.buffer.spi.PartitionFactory;
import org.apache.directmemory.buffer.spi.PartitionSliceSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UnsafePooledPartition
    extends AbstractPooledPartition
{

    public static final PartitionFactory UNSAFE_PARTITION_FACTORY = new PartitionFactory()
    {

        @Override
        public Partition newPartition( int partitionIndex, int sliceByteSize, int slices,
                                       PartitionSliceSelector partitionSliceSelector )
        {
            return new UnsafePooledPartition( partitionIndex, slices, sliceByteSize, partitionSliceSelector );
        }
    };

    private static final Logger LOGGER = LoggerFactory.getLogger( UnsafePooledPartition.class );

    private final long allocatedLength;

    private final UnsafePartitionSlice[] slices;

    private UnsafePooledPartition( int partitionIndex, int slices, int sliceByteSize,
                                   PartitionSliceSelector partitionSliceSelector )
    {
        super( partitionIndex, slices, sliceByteSize, partitionSliceSelector, true );

        this.slices = new UnsafePartitionSlice[slices];
        this.allocatedLength = sliceByteSize * slices;

        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "malloc data: partitionIndex=" + partitionIndex + ", allocatedLength=" + allocatedLength );
        }

        for ( int i = 0; i < slices; i++ )
        {
            this.slices[i] = new UnsafePartitionSlice( i, this, sliceByteSize );

            if ( LOGGER.isTraceEnabled() )
            {
                UnsafePartitionSlice slice = this.slices[i];
                LOGGER.trace( "sliced data: memoryPointer=" + ( slice.memoryPointer ) + ", sliceIndex=" + i
                    + ", length=" + sliceByteSize + ", lastBytePointer=" + slice.lastMemoryPointer );
            }
        }
    }

    @Override
    protected AbstractPartitionSlice get( int index )
    {
        return slices[index];
    }

}
