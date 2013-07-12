package org.apache.directmemory.memory.buffer;

import org.apache.directmemory.buffer.PartitionStrategy;
import org.apache.directmemory.buffer.impl.BufferUtils;
import org.apache.directmemory.buffer.spi.PartitionFactory;

final class PartitionBufferMemoryManagerHelper
{

    private static final String PB_PROPERTY_DISABLE_POOLING = "directmemory.buffer.pooling.disabled";

    private static final String PB_PROPERTY_ENABLE_UNSAFE = "directmemory.buffer.unsafe.enabled";

    private static final String PB_PROPERTY_ENABLE_OFFHEAP = "directmemory.buffer.offheap.enabled";

    private PartitionBufferMemoryManagerHelper()
    {
    }

    static PartitionFactory applyStrategies()
    {
        String property = System.getProperty( PB_PROPERTY_ENABLE_UNSAFE, "false" );
        boolean enableUnsafe = Boolean.parseBoolean( property ) && BufferUtils.isUnsafeAvailable();

        property = System.getProperty( PB_PROPERTY_ENABLE_OFFHEAP, "false" );
        boolean enableOffheap = Boolean.parseBoolean( property );

        property = System.getProperty( PB_PROPERTY_DISABLE_POOLING, "false" );
        boolean disablePooling = Boolean.parseBoolean( property );

        if ( !disablePooling )
        {
            if ( enableUnsafe )
            {
                return PartitionStrategy.POOLED_UNSAFE.getPartitionFactory();
            }
            if ( enableOffheap )
            {
                return PartitionStrategy.POOLED_BYTEBUFFER_DIRECT.getPartitionFactory();
            }
            return PartitionStrategy.POOLED_BYTEBUFFER_HEAP.getPartitionFactory();
        }

        if ( enableUnsafe )
        {
            return PartitionStrategy.UNPOOLED_UNSAFE.getPartitionFactory();
        }
        if ( enableOffheap )
        {
            return PartitionStrategy.UNPOOLED_BYTEBUFFER_DIRECT.getPartitionFactory();
        }
        return PartitionStrategy.UNPOOLED_BYTEBUFFER_HEAP.getPartitionFactory();
    }

}
