package org.apache.directmemory.utils.pool;

public interface ObjectPool<T>
{

    T take();

    void release( T pooledObject );

}
