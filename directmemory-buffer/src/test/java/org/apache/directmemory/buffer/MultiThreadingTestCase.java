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

import org.apache.directmemory.buffer.spi.PartitionFactory;
import org.apache.directmemory.buffer.spi.PartitionSliceSelector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@RunWith( Parameterized.class )
public class MultiThreadingTestCase
{

    @Parameters( name = "Execution {index} - {0}, {1}" )
    public static Collection<Object[]> parameters()
    {
        return TestCaseConstants.EXECUTION_PARAMETER_MUTATIONS;
    }

    private final PartitionFactory partitionFactory;

    private final PartitionSliceSelector partitionSliceSelector;

    private final String executionCombiation;

    public MultiThreadingTestCase( String name1, String name2, PartitionFactory partitionFactory,
                                   Class<PartitionSliceSelector> partitionSliceSelectorClass )
        throws InstantiationException, IllegalAccessException
    {
        this.executionCombiation = "Execution {" + name1 + "}, {" + name2 + "}";
        this.partitionFactory = partitionFactory;
        this.partitionSliceSelector = partitionSliceSelectorClass.newInstance();
    }

    @Test
    public void testMultithreading()
        throws Exception
    {
        final int processorCount = Runtime.getRuntime().availableProcessors();

        final PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, partitionSliceSelector );
        final PartitionBufferPool pool = builder.allocatePool( "1M", processorCount * 8, "8K" );

        final ExecutorService executorService = Executors.newFixedThreadPool( processorCount * 2 + 1 );

        final CountDownLatch latch = new CountDownLatch( processorCount * 5 );

        final AtomicLong fullRuns = new AtomicLong( 0 );
        final AtomicLong executedRuns = new AtomicLong( 0 );
        final AtomicInteger lastMentionedPercent = new AtomicInteger( 0 );

        for ( int i = 0; i < processorCount * 5; i++ )
        {
            final int index = i;
            executorService.execute( new Runnable()
            {
                private final Random random = new Random( -System.nanoTime() );

                private final int maxRuns = 400;

                private volatile int o = 0;

                {
                    fullRuns.addAndGet( maxRuns );
                    System.out.println( "#" + index + " started with " + maxRuns + " runs." );
                }

                public void run()
                {
                    try
                    {
                        PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
                        if ( o >= maxRuns )
                        {
                            latch.countDown();
                            return;
                        }

                        final byte[] block = new byte[10 * ( 1024 + random.nextInt( 1024 ) )];
                        partitionBuffer.writeBytes( fill( block ) );

                        o++;

                        try
                        {
                            Thread.sleep( 15 );
                        }
                        catch ( Exception e )
                        {
                            // ignore
                        }

                        byte[] result = new byte[block.length];
                        partitionBuffer.readBytes( result );

                        for ( int i = 0; i < block.length; i++ )
                        {
                            if ( block[i] != result[i] )
                            {
                                System.err.println( "Arrays don't match at index=" + i );
                                break;
                            }
                        }

                        partitionBuffer.free();
                        executorService.execute( this );
                    }
                    catch ( Throwable t )
                    {
                        latch.countDown();
                    }

                    executedRuns.incrementAndGet();
                    int percent = (int) ( ( executedRuns.longValue() * 100 ) / fullRuns.longValue() );
                    int lastPercent = lastMentionedPercent.get();
                    if ( lastPercent + 4 < percent && lastMentionedPercent.compareAndSet( lastPercent, percent ) )
                    {
                        System.out.println( String.format( "Finished %s%%", percent ) );
                    }
                }

                private byte[] fill( byte[] block )
                {
                    for ( int i = 0; i < block.length; i++ )
                    {
                        block[i] = (byte) ( 120 + random.nextInt( 100 ) );
                    }
                    return block;
                }
            } );
        }

        latch.await();
        pool.close();
        executorService.shutdown();
    }

}
