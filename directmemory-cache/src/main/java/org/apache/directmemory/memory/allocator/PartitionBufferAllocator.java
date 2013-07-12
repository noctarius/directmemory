package org.apache.directmemory.memory.allocator;

import java.io.IOException;

import org.apache.directmemory.buffer.PartitionBuffer;
import org.apache.directmemory.buffer.PartitionBufferBuilder;
import org.apache.directmemory.buffer.PartitionBufferPool;
import org.apache.directmemory.buffer.spi.PartitionFactory;
import org.apache.directmemory.buffer.spi.PartitionSliceSelector;

public class PartitionBufferAllocator
    implements Allocator
{

    private final PartitionBufferPool bufferPool;

    public PartitionBufferAllocator( PartitionFactory partitionFactory, PartitionSliceSelector sliceSelector,
                                     int partitions, long memoryByteSize, int sliceByteSize )
    {
        PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, sliceSelector );
        bufferPool = builder.allocatePool( memoryByteSize, partitions, sliceByteSize );
    }

    @Override
    public void close()
        throws IOException
    {
        bufferPool.close();
    }

    @Override
    public void free( PartitionBuffer buffer )
    {
        bufferPool.freePartitionBuffer( buffer );
    }

    @Override
    public PartitionBuffer allocate( int size )
    {
        return bufferPool.getPartitionBuffer();
    }

    @Override
    public void clear()
    {
        throw new UnsupportedOperationException( "clear is not supported for preallocated pools" );
    }

    @Override
    public long getCapacity()
    {
        return bufferPool.getAllocatedMemory();
    }

    @Override
    public int getNumber()
    {
        return -1;
    }

}
