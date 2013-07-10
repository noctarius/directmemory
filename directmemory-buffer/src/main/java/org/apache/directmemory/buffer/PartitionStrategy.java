package org.apache.directmemory.buffer;

import org.apache.directmemory.buffer.impl.ByteBufferPooledPartition;
import org.apache.directmemory.buffer.impl.ByteBufferUnpooledPartition;
import org.apache.directmemory.buffer.impl.UnsafePooledPartition;
import org.apache.directmemory.buffer.impl.UnsafeUnpooledPartition;
import org.apache.directmemory.buffer.spi.PartitionFactory;

public enum PartitionStrategy
{

    POOLED_UNSAFE( UnsafePooledPartition.UNSAFE_PARTITION_FACTORY ), //
    POOLED_BYTEBUFFER_HEAP( ByteBufferPooledPartition.HEAP_BYTEBUFFER_PARTITION_FACTORY ), //
    POOLED_BYTEBUFFER_DIRECT( ByteBufferPooledPartition.DIRECT_BYTEBUFFER_PARTITION_FACTORY ), //
    UNPOOLED_UNSAFE( UnsafeUnpooledPartition.UNSAFE_PARTITION_FACTORY ), //
    UNPOOLED_BYTEBUFFER_HEAP( ByteBufferUnpooledPartition.HEAP_BYTEBUFFER_PARTITION_FACTORY ), //
    UNPOOLED_BYTEBUFFER_DIRECT( ByteBufferUnpooledPartition.DIRECT_BYTEBUFFER_PARTITION_FACTORY ), //
    ;

    private final PartitionFactory partitionFactory;

    private PartitionStrategy( PartitionFactory partitionFactory )
    {
        this.partitionFactory = partitionFactory;
    }

    public PartitionFactory getPartitionFactory()
    {
        return partitionFactory;
    }

}
