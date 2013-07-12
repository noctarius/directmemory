package org.apache.directmemory.memory.buffer;

import org.apache.directmemory.memory.MemoryManager;
import org.apache.directmemory.memory.MemoryManagerFactory;

public class PartitionBufferMemoryManagerFactory<V>
    implements MemoryManagerFactory<V>
{

    @Override
    public MemoryManager<V> build( int concurrencyLevel, int numberOfBuffers, int size )
    {
        return new PartitionBufferMemoryManager<V>( concurrencyLevel, numberOfBuffers, size );
    }

}
