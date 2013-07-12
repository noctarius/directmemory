package org.apache.directmemory.memory.unsafe;

import org.apache.directmemory.memory.MemoryManager;
import org.apache.directmemory.memory.MemoryManagerFactory;

public class UnsafeMemoryManagerFactory<V>
    implements MemoryManagerFactory<V>
{

    @Override
    public MemoryManager<V> build( int concurrencyLevel, int numberOfBuffers, int size )
    {
        UnsafeMemoryManager<V> memoryManager = new UnsafeMemoryManager<V>();
        memoryManager.init( numberOfBuffers, size );
        return memoryManager;
    }

}
