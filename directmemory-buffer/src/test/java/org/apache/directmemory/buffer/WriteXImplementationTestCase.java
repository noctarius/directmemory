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

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.directmemory.buffer.spi.PartitionFactory;
import org.apache.directmemory.buffer.spi.PartitionSliceSelector;
import org.apache.directmemory.buffer.utils.BufferUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith( Parameterized.class )
public class WriteXImplementationTestCase
{

    @Parameters( name = "Execution {index} - {0}, {1}" )
    public static Collection<Object[]> parameters()
    {
        List<Object[]> mutations = new ArrayList<Object[]>( TestCaseConstants.EXECUTION_PARAMETER_MUTATIONS.size() * 2 );
        for ( Object[] values : TestCaseConstants.EXECUTION_PARAMETER_MUTATIONS )
        {
            Object[] v = new Object[values.length + 1];
            System.arraycopy( values, 0, v, 0, values.length );
            v[values.length] = ByteOrder.BIG_ENDIAN;
            mutations.add( v );

            v = new Object[values.length + 1];
            System.arraycopy( values, 0, v, 0, values.length );
            v[values.length] = ByteOrder.LITTLE_ENDIAN;
            mutations.add( v );
        }

        return mutations;
    }

    private final PartitionFactory partitionFactory;

    private final PartitionSliceSelector partitionSliceSelector;

    private final ByteOrder byteOrder;

    public WriteXImplementationTestCase( String name1, String name2, PartitionFactory partitionFactory,
                                         Class<PartitionSliceSelector> partitionSliceSelectorClass, ByteOrder byteOrder )
        throws InstantiationException, IllegalAccessException
    {
        this.partitionFactory = partitionFactory;
        this.partitionSliceSelector = partitionSliceSelectorClass.newInstance();
        this.byteOrder = byteOrder;
    }

    @Test
    public void testWriteByte()
        throws Exception
    {
        PartitionBufferPool pool = buildPartitionBufferPool();
        try
        {
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            long bytes = BufferUtils.descriptorToByteSize( "256K" ) * 20;

            PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
            partitionBuffer.byteOrder( byteOrder );

            assertEquals( BufferUtils.descriptorToByteSize( "256K" ), partitionBuffer.maxCapacity() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            for ( int i = 0; i < bytes + 1; i++ )
            {
                partitionBuffer.writeByte( 7 );
                assertEquals( i + 1, partitionBuffer.writerIndex() );
            }
            partitionBuffer.flush();
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 21, partitionBuffer.maxCapacity() );
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 20 + 1, partitionBuffer.capacity() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            for ( int i = 0; i < bytes + 1; i++ )
            {
                try
                {
                    assertEquals( "Wrong value at position " + i, 7, partitionBuffer.readByte() );
                    assertEquals( i + 1, partitionBuffer.readerIndex() );
                }
                catch ( Throwable t )
                {
                    throw new Exception( "Failure at position " + i, t );
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

    @Test
    public void testWriteShort()
        throws Exception
    {
        PartitionBufferPool pool = buildPartitionBufferPool();
        try
        {
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            long bytes = BufferUtils.descriptorToByteSize( "256K" ) * 20;

            PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
            partitionBuffer.byteOrder( byteOrder );

            assertEquals( BufferUtils.descriptorToByteSize( "256K" ), partitionBuffer.maxCapacity() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            for ( int i = 0; i < bytes / 2 + 1; i++ )
            {
                partitionBuffer.writeShort( (short) 15555 );
                assertEquals( ( i + 1 ) * 2, partitionBuffer.writerIndex() );
            }
            partitionBuffer.flush();
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 21, partitionBuffer.maxCapacity() );
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 20 + 2, partitionBuffer.capacity() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            int rounds = (int) ( bytes / 2 + 1 );
            for ( int i = 0; i < bytes / 2 + 1; i++ )
            {
                try
                {
                    assertEquals( "Wrong value at position " + i, 15555, partitionBuffer.readShort() );
                    assertEquals( ( i + 1 ) * 2, partitionBuffer.readerIndex() );
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

    @Test
    public void testWriteInt()
        throws Exception
    {
        PartitionBufferPool pool = buildPartitionBufferPool();
        try
        {
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            long bytes = BufferUtils.descriptorToByteSize( "256K" ) * 20;

            PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
            partitionBuffer.byteOrder( byteOrder );

            assertEquals( BufferUtils.descriptorToByteSize( "256K" ), partitionBuffer.maxCapacity() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            for ( int i = 0; i < bytes / 4 + 1; i++ )
            {
                partitionBuffer.writeInt( 755550 );
                assertEquals( ( i + 1 ) * 4, partitionBuffer.writerIndex() );
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

    @Test
    public void testWriteFloat()
        throws Exception
    {
        PartitionBufferPool pool = buildPartitionBufferPool();
        try
        {
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            long bytes = BufferUtils.descriptorToByteSize( "256K" ) * 20;

            PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
            partitionBuffer.byteOrder( byteOrder );

            assertEquals( BufferUtils.descriptorToByteSize( "256K" ), partitionBuffer.maxCapacity() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            for ( int i = 0; i < bytes / 4 + 1; i++ )
            {
                partitionBuffer.writeFloat( 755550f );
                assertEquals( ( i + 1 ) * 4, partitionBuffer.writerIndex() );
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
                    int expected = Float.floatToIntBits( 755550f );
                    int result = Float.floatToIntBits( partitionBuffer.readFloat() );
                    assertEquals( "Wrong value at position " + i, expected, result );
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

    @Test
    public void testWriteLong()
        throws Exception
    {
        PartitionBufferPool pool = buildPartitionBufferPool();
        try
        {
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            long bytes = BufferUtils.descriptorToByteSize( "256K" ) * 20;

            PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
            partitionBuffer.byteOrder( byteOrder );

            assertEquals( BufferUtils.descriptorToByteSize( "256K" ), partitionBuffer.maxCapacity() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            for ( int i = 0; i < bytes / 8 + 1; i++ )
            {
                partitionBuffer.writeLong( 75555000L );
                assertEquals( ( i + 1 ) * 8, partitionBuffer.writerIndex() );
            }
            partitionBuffer.flush();
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 21, partitionBuffer.maxCapacity() );
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 20 + 8, partitionBuffer.capacity() );
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 20 + 8, partitionBuffer.writerIndex() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            int rounds = (int) ( bytes / 8 + 1 );
            for ( int i = 0; i < bytes / 8 + 1; i++ )
            {
                try
                {
                    assertEquals( "Wrong value at position " + i, 75555000L, partitionBuffer.readLong() );
                    assertEquals( ( i + 1 ) * 8, partitionBuffer.readerIndex() );
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

    @Test
    public void testWriteDouble()
        throws Exception
    {
        PartitionBufferPool pool = buildPartitionBufferPool();
        try
        {
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            long bytes = BufferUtils.descriptorToByteSize( "256K" ) * 20;

            PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
            partitionBuffer.byteOrder( byteOrder );

            assertEquals( BufferUtils.descriptorToByteSize( "256K" ), partitionBuffer.maxCapacity() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            for ( int i = 0; i < bytes / 8 + 1; i++ )
            {
                partitionBuffer.writeDouble( 755554540D );
                assertEquals( ( i + 1 ) * 8, partitionBuffer.writerIndex() );
            }
            partitionBuffer.flush();
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 21, partitionBuffer.maxCapacity() );
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 20 + 8, partitionBuffer.capacity() );
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 20 + 8, partitionBuffer.writerIndex() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            int rounds = (int) ( bytes / 8 + 1 );
            for ( int i = 0; i < bytes / 8 + 1; i++ )
            {
                try
                {
                    long expected = Double.doubleToLongBits( 755554540D );
                    long result = Double.doubleToLongBits( partitionBuffer.readDouble() );
                    assertEquals( "Wrong value at position " + i, expected, result );
                    assertEquals( ( i + 1 ) * 8, partitionBuffer.readerIndex() );
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

    private PartitionBufferPool buildPartitionBufferPool()
    {
        PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, partitionSliceSelector );
        return builder.allocatePool( "50M", 50, "256K" );
    }

}
