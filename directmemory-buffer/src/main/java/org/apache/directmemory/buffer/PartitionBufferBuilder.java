package org.apache.directmemory.buffer;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.directmemory.buffer.impl.ByteBufferPooledPartition;
import org.apache.directmemory.buffer.impl.PartitionBufferPoolImpl;
import org.apache.directmemory.buffer.selector.ThreadLocalPartitionSliceSelector;
import org.apache.directmemory.buffer.spi.PartitionFactory;
import org.apache.directmemory.buffer.spi.PartitionSliceSelector;
import org.apache.directmemory.buffer.utils.BufferUtils;

public final class PartitionBufferBuilder
{

    private static final String NOT_MULTIPLE_OF_EIGHT = "%s is not a multiple of 8";
    private static final String SIZE_PER_X_EXCEEDS_MAX = "Byte size per %s exceeds allowed %s maximum";
    private static final String X_NOT_DIVISOR_OF_Y = "%s is not a divisor of %s";

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
        if ( !BufferUtils.isMultipleOfEight(sliceByteSize) )
        {
            throwNotMultipleOfEight( "sliceByteSize" );
        }
        if ( sliceByteSize > Integer.MAX_VALUE )
        {
            throwSizePerXExceedsMax( "slice" );
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
        if ( !BufferUtils.isMultipleOfEight(sliceByteSize) )
        {
            throwNotMultipleOfEight( "sliceByteSize" );
        }
        if ( sliceByteSize > Integer.MAX_VALUE )
        {
            throwSizePerXExceedsMax( "slice" );
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
        if ( !BufferUtils.isMultipleOfEight(memoryByteSize) )
        {
            throwNotMultipleOfEight( "memoryByteSize" );
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
            throwNotDivisorOf( "partitions", "memoryByteSize" );
        }

        long partitionByteSize = memoryByteSize / partitions;
        if ( !BufferUtils.isMultipleOfEight(partitionByteSize) )
        {
            throwNotMultipleOfEight( "partitionByteSize" );
        }
        if ( partitionByteSize > Integer.MAX_VALUE )
        {
            throwSizePerXExceedsMax( "partition" );
        }

        if ( partitionByteSize % sliceByteSize != 0 )
        {
            throwNotDivisorOf( "sliceByteSize", "partitionByteSize" );
        }

        int slices = (int) ( partitionByteSize / sliceByteSize );
        return new PartitionBufferPoolImpl( partitions, sliceByteSize, slices, partitionFactory, partitionSliceSelector );
    }

    private void throwNotMultipleOfEight( String parameter ) {
        throw new IllegalArgumentException( String.format( NOT_MULTIPLE_OF_EIGHT, parameter ) );
    }

    private void throwSizePerXExceedsMax( String parameter ) {
        throw new IllegalArgumentException( String.format( SIZE_PER_X_EXCEEDS_MAX, parameter, parameter ) );
    }

    private void throwNotDivisorOf( String parameter1, String parameter2 ) {
        throw new IllegalArgumentException( String.format( X_NOT_DIVISOR_OF_Y, parameter1, parameter2 ) );
    }

}
