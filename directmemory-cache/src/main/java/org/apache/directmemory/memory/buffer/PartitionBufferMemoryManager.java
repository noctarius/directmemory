package org.apache.directmemory.memory.buffer;

import java.io.IOException;

import org.apache.directmemory.buffer.PartitionBufferBuilder;
import org.apache.directmemory.buffer.PartitionBufferPool;
import org.apache.directmemory.buffer.selector.ThreadLocalPartitionSliceSelector;
import org.apache.directmemory.buffer.spi.PartitionFactory;
import org.apache.directmemory.buffer.spi.PartitionSliceSelector;
import org.apache.directmemory.memory.AbstractMemoryManager;
import org.apache.directmemory.memory.MemoryManager;
import org.apache.directmemory.memory.Pointer;

public class PartitionBufferMemoryManager<V>
    extends AbstractMemoryManager<V>
    implements MemoryManager<V>
{

    private final PartitionBufferPool bufferPool;

    private final int concurrencyLevel;

    private final int numberOfBuffers;

    private final int sliceByteSize;

    public PartitionBufferMemoryManager( int concurrencyLevel, int numberOfBuffers, int sliceByteSize )
    {
        this( PartitionBufferMemoryManagerHelper.applyStrategies(), concurrencyLevel, numberOfBuffers, sliceByteSize );
    }

    public PartitionBufferMemoryManager( PartitionFactory partitionFactory, int concurrencyLevel, int numberOfBuffers,
                                         int sliceByteSize )
    {
        this( partitionFactory, new ThreadLocalPartitionSliceSelector(), concurrencyLevel, numberOfBuffers,
              sliceByteSize );
    }

    public PartitionBufferMemoryManager( PartitionFactory partitionFactory, PartitionSliceSelector sliceSelector,
                                         int concurrencyLevel, int numberOfBuffers, int sliceByteSize )
    {
        this.concurrencyLevel = concurrencyLevel;
        this.numberOfBuffers = numberOfBuffers;
        this.sliceByteSize = sliceByteSize;
        PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, sliceSelector );
        long memoryByteSize = numberOfBuffers * sliceByteSize;
        bufferPool = builder.allocatePool( memoryByteSize, concurrencyLevel, sliceByteSize );
    }

    @Override
    public void close()
        throws IOException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public byte[] retrieve( Pointer<V> pointer )
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void clear()
    {
        // TODO Auto-generated method stub

    }

    @Override
    public long capacity()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Pointer<V> store( byte[] payload, long expiresIn )
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pointer<V> free( Pointer<V> pointer )
    {
        // TODO Auto-generated method stub
        return null;
    }

}
