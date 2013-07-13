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

import java.nio.ByteBuffer;

import org.apache.directmemory.buffer.spi.Partition;
import org.apache.directmemory.buffer.spi.PartitionFactory;
import org.apache.directmemory.buffer.spi.PartitionSliceSelector;

public class ByteBufferUnpooledPartition
    extends AbstractUnpooledPartition
{

    public static final PartitionFactory DIRECT_BYTEBUFFER_PARTITION_FACTORY = new PartitionFactory()
    {

        @Override
        public Partition newPartition( int partitionIndex, int sliceByteSize, int slices,
                                       PartitionSliceSelector partitionSliceSelector )
        {
            return new ByteBufferUnpooledPartition( partitionIndex, slices, sliceByteSize, true, partitionSliceSelector );
        }
    };

    public static final PartitionFactory HEAP_BYTEBUFFER_PARTITION_FACTORY = new PartitionFactory()
    {

        @Override
        public Partition newPartition( int partitionIndex, int sliceByteSize, int slices,
                                       PartitionSliceSelector partitionSliceSelector )
        {
            return new ByteBufferUnpooledPartition( partitionIndex, slices, sliceByteSize, false,
                                                    partitionSliceSelector );
        }
    };

    private final boolean directMemory;

    private ByteBufferUnpooledPartition( int partitionIndex, int slices, int sliceByteSize, boolean directMemory,
                                         PartitionSliceSelector partitionSliceSelector )
    {
        super( partitionIndex, slices, sliceByteSize, partitionSliceSelector, false );

        this.directMemory = directMemory;
    }

    @Override
    protected AbstractPartitionSlice createPartitionSlice( int index, int sliceByteSize )
    {
        ByteBuffer buffer =
            directMemory ? ByteBuffer.allocateDirect( sliceByteSize ) : ByteBuffer.allocate( sliceByteSize );
        return new ByteBufferPartitionSlice( buffer, nextSlice(), this, sliceByteSize );
    }

}
