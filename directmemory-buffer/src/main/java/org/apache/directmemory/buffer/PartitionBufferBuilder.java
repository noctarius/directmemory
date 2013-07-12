package org.apache.directmemory.buffer;

import org.apache.directmemory.buffer.impl.BufferUtils;
import org.apache.directmemory.buffer.impl.ByteBufferPooledPartition;
import org.apache.directmemory.buffer.impl.PartitionBufferPoolImpl;
import org.apache.directmemory.buffer.selector.ThreadLocalPartitionSliceSelector;
import org.apache.directmemory.buffer.spi.PartitionFactory;
import org.apache.directmemory.buffer.spi.PartitionSliceSelector;

public final class PartitionBufferBuilder
{

    private static final int DEFAULT_PARTITIONS_COUNT = Runtime.getRuntime().availableProcessors();

    private final PartitionFactory partitionFactory;

    private final PartitionSliceSelector partitionSliceSelector;

    public PartitionBufferBuilder( PartitionFactory partitionFactory, PartitionSliceSelector partitionSliceSelector )
    {
        this.partitionFactory = partitionFactory;
        this.partitionSliceSelector = partitionSliceSelector;
    }

    public PartitionBufferBuilder( PartitionStrategy partitionStrategy, PartitionSliceSelector partitionSliceSelector )
    {
        this.partitionFactory = partitionStrategy.getPartitionFactory();
        this.partitionSliceSelector = partitionSliceSelector;
    }

    public PartitionBufferBuilder( PartitionFactory partitionFactory )
    {
        this( partitionFactory, new ThreadLocalPartitionSliceSelector() );
    }

    public PartitionBufferBuilder( PartitionStrategy partitionStrategy )
    {
        this( partitionStrategy.getPartitionFactory(), new ThreadLocalPartitionSliceSelector() );
    }

    public PartitionBufferBuilder( PartitionSliceSelector partitionSliceSelector )
    {
        this( ByteBufferPooledPartition.DIRECT_BYTEBUFFER_PARTITION_FACTORY, partitionSliceSelector );
    }

    public PartitionBufferPool allocatePool( String memorySizeDescriptor, String sliceSizeDescriptor )
    {
        return allocatePool( memorySizeDescriptor, DEFAULT_PARTITIONS_COUNT, sliceSizeDescriptor );
    }

    public PartitionBufferPool allocatePool( String memorySizeDescriptor, int partitions, String sliceSizeDescriptor )
    {
        long sliceByteSize = BufferUtils.descriptorToByteSize( sliceSizeDescriptor );
        if ( !BufferUtils.isPowerOfTwo( sliceByteSize ) )
        {
            throw new IllegalArgumentException( "sliceByteSize is not a power of 2" );
        }
        if ( sliceByteSize > Integer.MAX_VALUE )
        {
            throw new IllegalArgumentException( "Bytesize per slice will be a value larger than allowed slice maximum" );
        }

        return allocatePool( memorySizeDescriptor, partitions, (int) sliceByteSize );
    }

    public PartitionBufferPool allocatePool( long memoryByteSize, String sliceSizeDescriptor )
    {
        return allocatePool( memoryByteSize, DEFAULT_PARTITIONS_COUNT, sliceSizeDescriptor );
    }

    public PartitionBufferPool allocatePool( long memoryByteSize, int partitions, String sliceSizeDescriptor )
    {
        long sliceByteSize = BufferUtils.descriptorToByteSize( sliceSizeDescriptor );
        if ( !BufferUtils.isPowerOfTwo( sliceByteSize ) )
        {
            throw new IllegalArgumentException( "sliceByteSize is not a power of 2" );
        }
        if ( sliceByteSize > Integer.MAX_VALUE )
        {
            throw new IllegalArgumentException( "Bytesize per slice will be a value larger than allowed slice maximum" );
        }

        return allocatePool( memoryByteSize, partitions, (int) sliceByteSize );
    }

    public PartitionBufferPool allocatePool( String memorySizeDescriptor, int sliceByteSize )
    {
        return allocatePool( memorySizeDescriptor, DEFAULT_PARTITIONS_COUNT, sliceByteSize );
    }

    public PartitionBufferPool allocatePool( String memorySizeDescriptor, int partitions, int sliceByteSize )
    {
        long memoryByteSize = BufferUtils.descriptorToByteSize( memorySizeDescriptor );
        if ( !BufferUtils.isPowerOfTwo( memoryByteSize ) )
        {
            throw new IllegalArgumentException( "memoryByteSize is not a power of 2" );
        }

        return allocatePool( memoryByteSize, partitions, sliceByteSize );
    }

    public PartitionBufferPool allocatePool( long memoryByteSize, int sliceByteSize )
    {
        return allocatePool( memoryByteSize, DEFAULT_PARTITIONS_COUNT, sliceByteSize );
    }

    public PartitionBufferPool allocatePool( long memoryByteSize, int partitions, int sliceByteSize )
    {
        if ( partitions == 0 )
        {
            throw new IllegalArgumentException( "partitions must be greater / equal one" );
        }

        if ( memoryByteSize % partitions != 0 )
        {
            throw new IllegalArgumentException( "partitions is not a divisor of memoryByteSize" );
        }

        long partitionByteSize = memoryByteSize / partitions;
        if ( !BufferUtils.isPowerOfTwo( partitionByteSize ) )
        {
            throw new IllegalArgumentException( "partitionByteSize is not a power of 2" );
        }
        if ( partitionByteSize > Integer.MAX_VALUE )
        {
            throw new IllegalArgumentException(
                                                "Bytesize per partition will be a value larger than allowed partition maximum" );
        }

        if ( partitionByteSize % sliceByteSize != 0 )
        {
            throw new IllegalArgumentException( "sliceByteSize is not a divisor of the bytesize per partition" );
        }

        int slices = (int) ( partitionByteSize / sliceByteSize );
        return new PartitionBufferPoolImpl( partitions, sliceByteSize, slices, partitionFactory, partitionSliceSelector );
    }

}