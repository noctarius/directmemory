package org.apache.directmemory.memory.buffer;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.directmemory.buffer.PartitionBuffer;
import org.apache.directmemory.buffer.PartitionBufferBuilder;
import org.apache.directmemory.buffer.PartitionBufferPool;
import org.apache.directmemory.buffer.selector.ThreadLocalPartitionSliceSelector;
import org.apache.directmemory.buffer.spi.PartitionFactory;
import org.apache.directmemory.buffer.spi.PartitionSliceSelector;
import org.apache.directmemory.memory.AbstractMemoryManager;
import org.apache.directmemory.memory.MemoryManager;
import org.apache.directmemory.memory.Pointer;
import org.apache.directmemory.memory.PointerImpl;

public class PartitionBufferMemoryManager<V>
    extends AbstractMemoryManager<V>
    implements MemoryManager<V>
{

    private final List<PointerImpl<?>> pointers = new CopyOnWriteArrayList<PointerImpl<?>>();

    private final PartitionBufferPool bufferPool;

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
        PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, sliceSelector );
        long memoryByteSize = numberOfBuffers * sliceByteSize;
        bufferPool = builder.allocatePool( memoryByteSize, concurrencyLevel, sliceByteSize );
    }

    @Override
    public void close()
        throws IOException
    {
        bufferPool.close();
    }

    @Override
    public byte[] retrieve( Pointer<V> pointer )
    {
        PartitionBuffer buffer = pointer.getPartitionBuffer();
        long dataLength = buffer.writerIndex();
        if ( dataLength > Integer.MAX_VALUE )
        {
            throw new IllegalStateException( "The underlying data length is bigger as max size of an byte array" );
        }
        byte[] data = new byte[(int) dataLength];
        buffer.readBytes( data );
        return data;
    }

    @Override
    public void clear()
    {
        Iterator<PointerImpl<?>> iterator = pointers.iterator();
        while ( iterator.hasNext() )
        {
            PointerImpl<?> pointer = iterator.next();
            PartitionBuffer buffer = pointer.getPartitionBuffer();
            buffer.free();
            iterator.remove();
        }
    }

    @Override
    public long capacity()
    {
        return bufferPool.getAllocatedMemory();
    }

    @Override
    public Pointer<V> store( byte[] payload, long expiresIn )
    {
        PartitionBuffer buffer = bufferPool.getPartitionBuffer();
        PointerImpl<V> pointer = new PointerImpl<V>( buffer, 1 );
        pointer.expiresIn = expiresIn;
        buffer.writeBytes( payload );
        return pointer;
    }

    @Override
    public Pointer<V> free( Pointer<V> pointer )
    {
        PartitionBuffer buffer = pointer.getPartitionBuffer();
        buffer.free();
        pointers.remove( pointer );
        return pointer;
    }

}
