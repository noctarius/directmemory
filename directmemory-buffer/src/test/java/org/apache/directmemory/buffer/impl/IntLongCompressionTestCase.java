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

import static org.apache.directmemory.buffer.utils.Int32Compressor.INT32_FULL;
import static org.apache.directmemory.buffer.utils.Int32Compressor.INT32_MAX_DOUBLE;
import static org.apache.directmemory.buffer.utils.Int32Compressor.INT32_MAX_SINGLE;
import static org.apache.directmemory.buffer.utils.Int32Compressor.INT32_MAX_TRIPPLE;
import static org.apache.directmemory.buffer.utils.Int32Compressor.INT32_MIN_DOUBLE;
import static org.apache.directmemory.buffer.utils.Int32Compressor.INT32_MIN_SINGLE;
import static org.apache.directmemory.buffer.utils.Int32Compressor.INT32_MIN_TRIPPLE;
import static org.apache.directmemory.buffer.utils.Int64Compressor.INT64_FULL;
import static org.apache.directmemory.buffer.utils.Int64Compressor.INT64_MAX_DOUBLE;
import static org.apache.directmemory.buffer.utils.Int64Compressor.INT64_MAX_FIFTH;
import static org.apache.directmemory.buffer.utils.Int64Compressor.INT64_MAX_QUAD;
import static org.apache.directmemory.buffer.utils.Int64Compressor.INT64_MAX_SEVENTH;
import static org.apache.directmemory.buffer.utils.Int64Compressor.INT64_MAX_SINGLE;
import static org.apache.directmemory.buffer.utils.Int64Compressor.INT64_MAX_SIXTH;
import static org.apache.directmemory.buffer.utils.Int64Compressor.INT64_MAX_TRIPPLE;
import static org.apache.directmemory.buffer.utils.Int64Compressor.INT64_MIN_DOUBLE;
import static org.apache.directmemory.buffer.utils.Int64Compressor.INT64_MIN_FIFTH;
import static org.apache.directmemory.buffer.utils.Int64Compressor.INT64_MIN_QUAD;
import static org.apache.directmemory.buffer.utils.Int64Compressor.INT64_MIN_SEVENTH;
import static org.apache.directmemory.buffer.utils.Int64Compressor.INT64_MIN_SINGLE;
import static org.apache.directmemory.buffer.utils.Int64Compressor.INT64_MIN_SIXTH;
import static org.apache.directmemory.buffer.utils.Int64Compressor.INT64_MIN_TRIPPLE;
import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.Random;

import org.apache.directmemory.buffer.PartitionBuffer;
import org.apache.directmemory.buffer.PartitionBufferBuilder;
import org.apache.directmemory.buffer.PartitionBufferPool;
import org.apache.directmemory.buffer.TestCaseConstants;
import org.apache.directmemory.buffer.spi.PartitionFactory;
import org.apache.directmemory.buffer.spi.PartitionSliceSelector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith( Parameterized.class )
public class IntLongCompressionTestCase
{

    @Parameters( name = "Execution {index} - {0}, {1}" )
    public static Collection<Object[]> parameters()
    {
        return TestCaseConstants.EXECUTION_PARAMETER_MUTATIONS;
    }

    private final PartitionFactory partitionFactory;

    private final PartitionSliceSelector partitionSliceSelector;

    public IntLongCompressionTestCase( String name1, String name2, PartitionFactory partitionFactory,
                                       Class<PartitionSliceSelector> partitionSliceSelectorClass )
        throws InstantiationException, IllegalAccessException
    {
        this.partitionFactory = partitionFactory;
        this.partitionSliceSelector = partitionSliceSelectorClass.newInstance();
    }

    private static final int TEST_VALUES_COUNT = 1000000;

    @Test
    public void testInt32Compression()
        throws Exception
    {
        PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, partitionSliceSelector );
        PartitionBufferPool pool = builder.allocatePool( "50K", 1, "50K" );
        try
        {
            for ( int i = 0; i < 4; i++ )
            {
                int[] values = new int[TEST_VALUES_COUNT];

                Random random = new Random( -System.currentTimeMillis() );
                int min = ( 0x1 << ( i * 8 ) );
                int max = ( 0xFF << ( i * 8 ) );
                for ( int o = 0; o < TEST_VALUES_COUNT; o++ )
                {
                    values[o] = (int) ( random.nextDouble() * ( max - min + 1 ) ) + min;
                }

                PartitionBuffer buffer = pool.getPartitionBuffer();

                for ( int v = 0; v < TEST_VALUES_COUNT; v++ )
                {
                    int value = values[v];
                    buffer.clear();
                    buffer.writeCompressedInt( value );
                    buffer.flush();
                    checkIntLength( value, buffer.writerIndex() );

                    int result = buffer.readCompressedInt();
                    assertEquals( value, result );
                }
                buffer.free();
            }
        }
        finally
        {
            pool.close();
        }
    }

    @Test
    public void testInt64Compression()
        throws Exception
    {

        PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, partitionSliceSelector );
        PartitionBufferPool pool = builder.allocatePool( "50K", 1, "50K" );
        try
        {
            for ( int i = 0; i < 8; i++ )
            {
                long[] values = new long[TEST_VALUES_COUNT];

                Random random = new Random( -System.currentTimeMillis() );
                int min = ( 0x1 << ( i * 8 ) );
                int max = ( 0xFF << ( i * 8 ) );
                for ( int o = 0; o < TEST_VALUES_COUNT; o++ )
                {
                    values[o] = (long) ( random.nextDouble() * ( max - min + 1 ) ) + min;
                }

                PartitionBuffer buffer = pool.getPartitionBuffer();

                for ( int v = 0; v < TEST_VALUES_COUNT; v++ )
                {
                    long value = values[v];
                    buffer.clear();
                    buffer.writeCompressedLong( value );
                    buffer.flush();
                    checkLongLength( value, buffer.writerIndex() );

                    long result = buffer.readCompressedLong();
                    assertEquals( value, result );
                }
                buffer.free();
            }
        }
        finally
        {
            pool.close();
        }
    }

    private void checkIntLength( int value, long delta )
    {
        if ( value >= INT32_MIN_SINGLE && value <= INT32_MAX_SINGLE )
        {
            assertEquals( "Compressed bytesize for value " + value + " is too large", 2, delta );
        }
        else if ( value >= INT32_MIN_DOUBLE && value <= INT32_MAX_DOUBLE )
        {
            assertEquals( "Compressed bytesize for value " + value + " is too large", 3, delta );
        }
        else if ( value >= INT32_MIN_TRIPPLE && value <= INT32_MAX_TRIPPLE )
        {
            assertEquals( "Compressed bytesize for value " + value + " is too large", 4, delta );
        }
        else if ( value >= INT32_FULL && value <= INT32_FULL )
        {
            assertEquals( "Compressed bytesize for value " + value + " is too large", 5, delta );
        }
    }

    private void checkLongLength( long value, long delta )
    {
        if ( value >= INT64_MIN_SINGLE && value <= INT64_MAX_SINGLE )
        {
            assertEquals( "Compressed bytesize for value " + value + " is too large", 2, delta );
        }
        else if ( value >= INT64_MIN_DOUBLE && value <= INT64_MAX_DOUBLE )
        {
            assertEquals( "Compressed bytesize for value " + value + " is too large", 3, delta );
        }
        else if ( value >= INT64_MIN_TRIPPLE && value <= INT64_MAX_TRIPPLE )
        {
            assertEquals( "Compressed bytesize for value " + value + " is too large", 4, delta );
        }
        else if ( value >= INT64_MIN_QUAD && value <= INT64_MAX_QUAD )
        {
            assertEquals( "Compressed bytesize for value " + value + " is too large", 5, delta );
        }
        else if ( value >= INT64_MIN_FIFTH && value <= INT64_MAX_FIFTH )
        {
            assertEquals( "Compressed bytesize for value " + value + " is too large", 6, delta );
        }
        else if ( value >= INT64_MIN_SIXTH && value <= INT64_MAX_SIXTH )
        {
            assertEquals( "Compressed bytesize for value " + value + " is too large", 7, delta );
        }
        else if ( value >= INT64_MIN_SEVENTH && value <= INT64_MAX_SEVENTH )
        {
            assertEquals( "Compressed bytesize for value " + value + " is too large", 8, delta );
        }
        else if ( value >= INT64_FULL && value <= INT64_FULL )
        {
            assertEquals( "Compressed bytesize for value " + value + " is too large", 9, delta );
        }
    }

}
