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

import java.util.Collection;

import org.apache.directmemory.buffer.PartitionBuffer;
import org.apache.directmemory.buffer.PartitionBufferBuilder;
import org.apache.directmemory.buffer.PartitionBufferPool;
import org.apache.directmemory.buffer.spi.PartitionFactory;
import org.apache.directmemory.buffer.spi.PartitionSliceSelector;
import org.apache.directmemory.buffer.utils.BufferUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith( Parameterized.class )
public class PartitionBufferTestCase
{

    @Parameters( name = "Execution {index} - {0}, {1}" )
    public static Collection<Object[]> parameters()
    {
        return TestCaseConstants.EXECUTION_PARAMETER_MUTATIONS;
    }

    private final PartitionFactory partitionFactory;

    private final PartitionSliceSelector partitionSliceSelector;

    public PartitionBufferTestCase( String name1, String name2, PartitionFactory partitionFactory,
                                    Class<PartitionSliceSelector> partitionSliceSelectorClass )
        throws InstantiationException, IllegalAccessException
    {
        this.partitionFactory = partitionFactory;
        this.partitionSliceSelector = partitionSliceSelectorClass.newInstance();
    }

    @Test
    public void testAllocation()
        throws Exception
    {
        PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, partitionSliceSelector );
        PartitionBufferPool pool = builder.allocatePool( "500M", 50, "512K" );

        try
        {
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
            assertEquals( BufferUtils.descriptorToByteSize( "512k" ), partitionBuffer.maxCapacity() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            for ( int i = 0; i < 1024 * 1024 + 1; i++ )
            {
                partitionBuffer.writeByte( 1 );
            }
            partitionBuffer.flush();
            assertEquals( BufferUtils.descriptorToByteSize( "512k" ) * 3, partitionBuffer.maxCapacity() );
            assertEquals( BufferUtils.descriptorToByteSize( "512k" ) * 2 + 1, partitionBuffer.capacity() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

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
    public void testAllocation2()
        throws Exception
    {
        PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, partitionSliceSelector );
        PartitionBufferPool pool = builder.allocatePool( "500M", 50, "256K" );

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
                partitionBuffer.writeByte( 1 );
            }
            partitionBuffer.flush();
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 21, partitionBuffer.maxCapacity() );
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 20 + 1, partitionBuffer.capacity() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            for ( int i = 0; i < bytes + 1; i++ )
            {
                assertEquals( "Wrong value at position " + i, 1, partitionBuffer.readByte() );
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

    @Test( expected = RuntimeException.class )
    public void testAllocationBufferFull()
        throws Exception
    {
        PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, partitionSliceSelector );
        PartitionBufferPool pool = builder.allocatePool( "1M", 1, "256K" );

        try
        {
            // Unpooled pools will never overflow but can throw OOM
            if ( !pool.isPooled() )
            {
                throw new RuntimeException( "Unpooled pools will never overflow but can throw OOM" );
            }

            for ( int o = 0; o < 100; o++ )
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
                    partitionBuffer.writeByte( 1 );
                }
                partitionBuffer.flush();
                assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 21, partitionBuffer.maxCapacity() );
                assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 20 + 1, partitionBuffer.capacity() );
                System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                    + " bytes), unused " + pool.getFreeSliceCount() );

                for ( int i = 0; i < bytes + 1; i++ )
                {
                    assertEquals( "Wrong value at position " + i, 1, partitionBuffer.readByte() );
                }

                pool.freePartitionBuffer( partitionBuffer );
                System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                    + " bytes), unused " + pool.getFreeSliceCount() );
            }
        }
        finally
        {
            pool.close();
        }
    }

    @Test
    public void testAllocationFullRound()
        throws Exception
    {
        PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, partitionSliceSelector );
        PartitionBufferPool pool = builder.allocatePool( "10M", 5, "256K" );

        try
        {
            for ( int o = 0; o < 10; o++ )
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
                    partitionBuffer.writeByte( 1 );
                }
                partitionBuffer.flush();
                assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 21, partitionBuffer.maxCapacity() );
                assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 20 + 1, partitionBuffer.capacity() );
                System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                    + " bytes), unused " + pool.getFreeSliceCount() );

                for ( int i = 0; i < bytes + 1; i++ )
                {
                    assertEquals( "Wrong value at position " + i, 1, partitionBuffer.readByte() );
                }

                pool.freePartitionBuffer( partitionBuffer );
                System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                    + " bytes), unused " + pool.getFreeSliceCount() );
            }
        }
        finally
        {
            pool.close();
        }
    }

}
