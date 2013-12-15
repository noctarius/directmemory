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

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.apache.directmemory.buffer.PartitionBuffer;
import org.apache.directmemory.buffer.PartitionBufferBuilder;
import org.apache.directmemory.buffer.PartitionBufferPool;
import org.apache.directmemory.buffer.spi.PartitionFactory;
import org.apache.directmemory.buffer.spi.PartitionSliceSelector;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.Clock;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;

@Ignore
@RunWith( Parameterized.class )
public class Benchmarker
    extends AbstractBenchmark
{

    @Parameters( name = "Execution {index} - {0}, {1}" )
    public static Collection<Object[]> parameters()
    {
        return TestCaseConstants.EXECUTION_PARAMETER_MUTATIONS;
    }

    private final Random random = new Random( -System.nanoTime() );

    private final PartitionSliceSelector partitionSliceSelector;

    private final PartitionBufferBuilder builder;

    private final PartitionBufferPool pool;

    public Benchmarker( String name1, String name2, PartitionFactory partitionFactory,
                        Class<PartitionSliceSelector> partitionSliceSelectorClass )
        throws InstantiationException, IllegalAccessException
    {
        this.partitionSliceSelector = partitionSliceSelectorClass.newInstance();

        this.builder = new PartitionBufferBuilder( partitionFactory, partitionSliceSelector );
        this.pool = builder.allocatePool( "20M", Runtime.getRuntime().availableProcessors() * 8, "8K" );
    }

    @Test
    @BenchmarkHistoryChart( labelWith = LabelType.RUN_ID, maxRuns = 20 )
    @BenchmarkOptions( warmupRounds = 1000, benchmarkRounds = 20000, clock = Clock.NANO_TIME, concurrency = 20 )
    public void benchmark()
        throws Exception
    {
        PartitionBuffer partitionBuffer = pool.getPartitionBuffer();

        try
        {
            byte[] block = new byte[10240];
            partitionBuffer.writeBytes( fill( block ) );
            partitionBuffer.flush();

            byte[] result = new byte[block.length];
            partitionBuffer.readBytes( result );

            for ( int i = 0; i < block.length; i++ )
            {
                if ( block[i] != result[i] )
                {
                    System.out.println( Arrays.toString( block ) );
                    System.out.println( Arrays.toString( result ) );
                    throw new Exception( "Arrays don't match at index=" + i );
                }
            }
        }
        finally
        {
            partitionBuffer.free();
        }
    }

    @Override
    protected void finalize()
    {
        pool.close();
    }

    private byte[] fill( byte[] block )
    {
        for ( int i = 0; i < block.length; i++ )
        {
            block[i] = (byte) ( 120 + random.nextInt( 100 ) );
        }
        return block;
    }

}
