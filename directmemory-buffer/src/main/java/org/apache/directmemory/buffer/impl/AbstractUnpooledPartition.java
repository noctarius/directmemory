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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.directmemory.buffer.spi.PartitionSlice;
import org.apache.directmemory.buffer.spi.PartitionSliceSelector;

public abstract class AbstractUnpooledPartition
    extends AbstractPartition
{

    protected AbstractUnpooledPartition( int partitionIndex, int slices, int sliceByteSize,
                                         PartitionSliceSelector partitionSliceSelector, boolean pooled )
    {
        super( partitionIndex, slices, sliceByteSize, partitionSliceSelector, pooled );
    }

    private final AtomicInteger index = new AtomicInteger( 0 );

    private final Map<Integer, AbstractPartitionSlice> bufferPartitionSlices =
        new ConcurrentHashMap<Integer, AbstractPartitionSlice>();

    @Override
    public int available()
    {
        return Integer.MAX_VALUE;
    }

    @Override
    public int used()
    {
        return bufferPartitionSlices.size();
    }

    @Override
    public int getSliceCount()
    {
        return bufferPartitionSlices.size();
    }

    @Override
    public PartitionSlice get()
    {
        AbstractPartitionSlice slice = createPartitionSlice( nextSlice(), sliceByteSize );
        bufferPartitionSlices.put( slice.index, slice );
        return slice;
    }

    @Override
    public void free( PartitionSlice slice )
    {
        if ( slice.getPartition() != this )
        {
            throw new IllegalArgumentException( "Given slice cannot be handled by this PartitionBufferPool" );
        }
        if ( !( slice instanceof AbstractPartitionSlice ) )
        {
            throw new IllegalArgumentException( "Given slice cannot be handled by this PartitionBufferPool" );
        }
        AbstractPartitionSlice partitionSlice = (AbstractPartitionSlice) slice;
        bufferPartitionSlices.remove( partitionSlice );
        partitionSlice.free();
    }

    @Override
    public void close()
    {
        if ( !close0() )
        {
            return;
        }

        Iterator<AbstractPartitionSlice> iterator = bufferPartitionSlices.values().iterator();
        while ( iterator.hasNext() )
        {
            iterator.next().free();
            iterator.remove();
        }
    }

    protected int nextSlice()
    {
        return index.incrementAndGet();
    }

    protected abstract AbstractPartitionSlice createPartitionSlice( int index, int sliceByteSize );

}
