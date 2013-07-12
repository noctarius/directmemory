package org.apache.directmemory.memory.allocator;

import org.apache.directmemory.memory.MemoryManager;
import org.apache.directmemory.memory.MemoryManagerFactory;

public class AllocatorMemoryManagerFactory<V>
    implements MemoryManagerFactory<V>
{

    @Override
    public MemoryManager<V> build( int concurrencyLevel, int numberOfBuffers, int size )
    {
        AllocatorMemoryManager<V> memoryManager = new AllocatorMemoryManager<V>();
        memoryManager.init( numberOfBuffers, size );
        return memoryManager;
    }

}
