package org.apache.directmemory.utils.pool;

public interface ObjectPoolItemFactory<T>
{

    T create();

    void activate( T pooledObject );

    void passivate( T pooledObject );

    void destroy( T pooledObject );

}
