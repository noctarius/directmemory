package org.apache.directmemory.utils.pool;

import org.apache.directmemory.buffer.impl.BufferUtils;

public class ObjectPoolFactory
{

    private static final String FASTOBJECTPOOL_BUILDER_CLASS =
        "org.apache.directmemory.utils.pool.FastObjectPoolBuilder";

    private static final String FURIOUSOBJECTPOOL_BUILDER_CLASS =
        "org.apache.directmemory.utils.pool.FuriousObjectPoolBuilder";

    private final ObjectPoolBuilder objectPoolBuilder;

    public static ObjectPoolFactory newInstance()
    {
        try
        {
            ClassLoader cl = ObjectPoolFactory.class.getClassLoader();
            Class<? extends ObjectPoolBuilder> builderClass;
            if ( BufferUtils.isUnsafeAvailable() )
            {
                builderClass = (Class<? extends ObjectPoolBuilder>) cl.loadClass( FASTOBJECTPOOL_BUILDER_CLASS );
            }
            else
            {
                builderClass = (Class<? extends ObjectPoolBuilder>) cl.loadClass( FURIOUSOBJECTPOOL_BUILDER_CLASS );
            }
            return new ObjectPoolFactory( builderClass.newInstance() );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Could not build ObjectPoolBuilder" );
        }
    }

    private ObjectPoolFactory( ObjectPoolBuilder objectPoolBuilder )
    {
        this.objectPoolBuilder = objectPoolBuilder;
    }

    static interface ObjectPoolBuilder
    {
        <T> ObjectPool<T> build( ObjectPoolItemFactory<T> factory, int size );
    }

}
