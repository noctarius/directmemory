package org.apache.directmemory.buffer.impl;

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

import java.nio.ByteOrder;
import java.util.Arrays;

import org.apache.directmemory.buffer.PartitionBuffer;
import org.apache.directmemory.buffer.PartitionBufferPool;
import org.apache.directmemory.buffer.spi.Partition;
import org.apache.directmemory.buffer.spi.PartitionFactory;
import org.apache.directmemory.buffer.spi.PartitionSlice;
import org.apache.directmemory.buffer.spi.PartitionSliceSelector;

public class PartitionBufferPoolImpl
    implements PartitionBufferPool
{

    private final PartitionSliceSelector partitionSliceSelector;

    private final Partition[] partitions;

    private final int sliceByteSize;

    private final int slices;

    public PartitionBufferPoolImpl( int partitions, int sliceByteSize, int slices, PartitionFactory partitionFactory,
                                    PartitionSliceSelector partitionSliceSelector )
    {

        this.partitions = new Partition[partitions];
        this.partitionSliceSelector = partitionSliceSelector;
        this.sliceByteSize = sliceByteSize;
        this.slices = slices;

        // Initialize partitions
        for ( int i = 0; i < partitions; i++ )
        {
            this.partitions[i] = partitionFactory.newPartition( i, sliceByteSize, slices, partitionSliceSelector );
        }
    }

    PartitionSlice requestSlice()
    {
        Partition[] partitionsCopy = Arrays.copyOf( partitions, partitions.length );
        return partitionSliceSelector.selectPartitionSlice( partitionsCopy );
    }

    void freeSlice( PartitionSlice slice )
    {
        if ( slice != null )
        {
            slice.getPartition().free( slice );
        }
    }

    @Override
    public boolean isPooled()
    {
        return partitions[0].isPooled();
    }

    @Override
    public PartitionBuffer getPartitionBuffer()
    {
        return new PoolPartitionBuffer( this, ByteOrder.BIG_ENDIAN );
    }

    @Override
    public void freePartitionBuffer( PartitionBuffer partitionBuffer )
    {
        partitionBuffer.free();
    }

    @Override
    public long getAllocatedMemory()
    {
        return getPartitionCount() * getPartitionByteSize();
    }

    @Override
    public int getPartitionByteSize()
    {
        return getSliceCountPerPartition() * getSliceByteSize();
    }

    @Override
    public int getPartitionCount()
    {
        return partitions.length;
    }

    @Override
    public int getSliceCountPerPartition()
    {
        return slices;
    }

    @Override
    public int getSliceCount()
    {
        return getSliceCountPerPartition() * getPartitionCount();
    }

    @Override
    public int getSliceByteSize()
    {
        return sliceByteSize;
    }

    @Override
    public int getUsedSliceCount()
    {
        int usedSlices = 0;
        for ( Partition partition : partitions )
        {
            usedSlices += partition.used();
        }
        return usedSlices;
    }

    @Override
    public int getFreeSliceCount()
    {
        int available = 0;
        for ( Partition partition : partitions )
        {
            available += partition.available();
        }
        return available;
    }

    @Override
    public void close()
    {
        for ( Partition partition : partitions )
        {
            partition.close();
        }
    }

}
