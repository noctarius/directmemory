package org.apache.directmemory.memory;

public interface MemoryManagerFactory<V>
{

    MemoryManager<V> build( int concurrencyLevel, int numberOfBuffers, int size );

}
