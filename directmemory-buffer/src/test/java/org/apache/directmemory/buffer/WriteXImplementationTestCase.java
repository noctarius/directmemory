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

import static org.junit.Assert.assertEquals;

import org.apache.directmemory.buffer.PartitionBuffer;
import org.apache.directmemory.buffer.PartitionBufferBuilder;
import org.apache.directmemory.buffer.PartitionBufferPool;
import org.apache.directmemory.buffer.impl.ByteBufferPooledPartition;
import org.apache.directmemory.buffer.selector.RoundRobinPartitionSliceSelector;
import org.apache.directmemory.buffer.spi.PartitionFactory;
import org.apache.directmemory.buffer.spi.PartitionSliceSelector;
import org.apache.directmemory.buffer.utils.BufferUtils;
import org.junit.Test;

public class WriteXImplementationTestCase
{

    @Test
    public void testWriteByte()
        throws Exception
    {
        PartitionSliceSelector partitionSliceSelector = new RoundRobinPartitionSliceSelector();
        PartitionFactory partitionFactory = ByteBufferPooledPartition.DIRECT_BYTEBUFFER_PARTITION_FACTORY;
        PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, partitionSliceSelector );
        PartitionBufferPool pool = builder.allocatePool( "50M", 50, "256K" );

        try
        {
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            long bytes = BufferUtils.descriptorToByteSize( "256K" ) * 20;

            PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ), partitionBuffer.maxCapacity() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            for ( int i = 0; i < bytes + 1; i++ )
            {
                partitionBuffer.writeByte( 7 );
            }
            partitionBuffer.flush();
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 21, partitionBuffer.maxCapacity() );
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 20 + 1, partitionBuffer.capacity() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            for ( int i = 0; i < bytes + 1; i++ )
            {
                assertEquals( "Wrong value at position " + i, 7, partitionBuffer.readByte() );
            }

            pool.freePartitionBuffer( partitionBuffer );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

        }
        finally
        {
            pool.close();
        }
    }

    @Test
    public void testWriteShort()
        throws Exception
    {
        PartitionSliceSelector partitionSliceSelector = new RoundRobinPartitionSliceSelector();
        PartitionFactory partitionFactory = ByteBufferPooledPartition.DIRECT_BYTEBUFFER_PARTITION_FACTORY;
        PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, partitionSliceSelector );
        PartitionBufferPool pool = builder.allocatePool( "50M", 50, "256K" );

        try
        {
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            long bytes = BufferUtils.descriptorToByteSize( "256K" ) * 20;

            PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ), partitionBuffer.maxCapacity() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            for ( int i = 0; i < bytes / 2 + 1; i++ )
            {
                partitionBuffer.writeShort( (short) 15555 );
            }
            partitionBuffer.flush();
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 21, partitionBuffer.maxCapacity() );
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 20 + 2, partitionBuffer.capacity() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            for ( int i = 0; i < bytes / 2 + 1; i++ )
            {
                assertEquals( "Wrong value at position " + i, 15555, partitionBuffer.readShort() );
            }

            pool.freePartitionBuffer( partitionBuffer );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

        }
        finally
        {
            pool.close();
        }
    }

    @Test
    public void testWriteInt()
        throws Exception
    {
        PartitionSliceSelector partitionSliceSelector = new RoundRobinPartitionSliceSelector();
        PartitionFactory partitionFactory = ByteBufferPooledPartition.HEAP_BYTEBUFFER_PARTITION_FACTORY;
        PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, partitionSliceSelector );
        PartitionBufferPool pool = builder.allocatePool( "50M", 50, "256K" );

        try
        {
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            long bytes = BufferUtils.descriptorToByteSize( "256K" ) * 20;

            PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ), partitionBuffer.maxCapacity() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            for ( int i = 0; i < bytes / 4 + 1; i++ )
            {
                partitionBuffer.writeInt( 755550 );
            }
            partitionBuffer.flush();
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 21, partitionBuffer.maxCapacity() );
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 20 + 4, partitionBuffer.capacity() );
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 20 + 4, partitionBuffer.writerIndex() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            int rounds = (int) ( bytes / 4 + 1 );
            for ( int i = 0; i < bytes / 4 + 1; i++ )
            {
                try
                {
                    assertEquals( "Wrong value at position " + i, 755550, partitionBuffer.readInt() );
                    assertEquals( ( i + 1 ) * 4, partitionBuffer.readerIndex() );
                }
                catch ( Throwable t )
                {
                    throw new Exception( "Failure at position " + i + " / " + rounds, t );
                }
            }

            pool.freePartitionBuffer( partitionBuffer );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

        }
        finally
        {
            pool.close();
        }
    }

}
