package org.apache.directmemory.memory;

public enum MemoryManagerStrategy
{

    Unsafe( "org.apache.directmemory.memory.unsafe.UnsafeMemoryManagerFactory" ), //
    Allocator( "org.apache.directmemory.memory.allocator.AllocatorMemoryManagerFactory" ), //
    PartitionBuffer( "" );

    private final String factoryClassName;

    private MemoryManagerStrategy( String factoryClassName )
    {
        this.factoryClassName = factoryClassName;
    }

    public <V> MemoryManagerFactory<V> newInstance()
    {
        try
        {
            Class<? extends MemoryManagerFactory<?>> clazz;
            ClassLoader cl = MemoryManagerStrategy.class.getClassLoader();
            clazz = (Class<? extends MemoryManagerFactory<?>>) cl.loadClass( factoryClassName );
            return (MemoryManagerFactory<V>) clazz.newInstance();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Could not build MemoryManagerFactory", e );
        }
    }

}
