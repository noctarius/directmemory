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

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.directmemory.buffer.PartitionBuffer;
import org.apache.directmemory.buffer.PartitionBufferBuilder;
import org.apache.directmemory.buffer.PartitionBufferPool;
import org.apache.directmemory.buffer.impl.UnsafePooledPartition;
import org.apache.directmemory.buffer.selector.ProcessorLocalPartitionSliceSelector;
import org.apache.directmemory.buffer.spi.PartitionFactory;
import org.apache.directmemory.buffer.spi.PartitionSliceSelector;

public class MultiThreadingTestCase
{

    public static void main( String[] args )
        throws Exception
    {
        final int processorCount = Runtime.getRuntime().availableProcessors();

        final PartitionFactory partitionFactory = UnsafePooledPartition.UNSAFE_PARTITION_FACTORY;
        final PartitionSliceSelector partitionSliceSelector = new ProcessorLocalPartitionSliceSelector();

        final PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, partitionSliceSelector );
        final PartitionBufferPool pool = builder.allocatePool( "1M", processorCount * 8, "8K" );

        final ExecutorService executorService = Executors.newFixedThreadPool( processorCount * 2 + 1 );

        final CountDownLatch latch = new CountDownLatch( processorCount * 5 );

        for ( int i = 0; i < processorCount * 5; i++ )
        {
            final int index = i;
            executorService.execute( new Runnable()
            {

                private final Random random = new Random( -System.nanoTime() );

                private final int maxRuns = index % 2 == 0 ? 10000 : 8000;

                private volatile int o = 0;

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
                            Thread.sleep( random.nextInt( 100 ) );
                        }
                        catch ( Exception e )
                        {
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
