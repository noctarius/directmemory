package org.apache.directmemory.preliminary;

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
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.directmemory.measures.Ram;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;

@AxisRange( min = 0, max = 1 )
@BenchmarkMethodChart( filePrefix = "latest-microbench" )
@BenchmarkOptions( benchmarkRounds = 1, warmupRounds = 0 )
@BenchmarkHistoryChart( labelWith = LabelType.CUSTOM_KEY, maxRuns = 5 )
@Ignore
public class MicroBenchmark
    extends AbstractBenchmark
{

    private static Logger logger = LoggerFactory.getLogger( MicroBenchmark.class );

    private final int many = 3000000;

    private final int less = 300000;

    @Before
    public void cleanup()
    {
        dump( "Before cleanup" );
        // Runtime.getRuntime().gc();
        // dump("After cleanup");
        logger.info( "************************************************" );
    }

    private void dump( String message )
    {
        logger.info( message );
        logger.info( "Memory - max: " + Ram.inMb( Runtime.getRuntime().maxMemory() ) );
        logger.info( "Memory - allocated: " + Ram.inMb( Runtime.getRuntime().totalMemory() ) );
        logger.info( "Memory - free : " + Ram.inMb( Runtime.getRuntime().freeMemory() ) );
    }

    @Test
    public void manySmallInHeapWithHashmap()
    {
        final Map<String, byte[]> test = Maps.newHashMap();
        final byte payload[] = new byte[450];
        long ops = many;
        for ( int i = 0; i < ops; i++ )
        {
            final String key = "test-" + i;
            test.put( key, payload.clone() );
        }
        logger.info( "many=" + ops );
        logger.info( "payload.length=" + payload.length );
        logger.info( "stored " + Ram.inMb( payload.length * ops ) );
    }

    @Test
    public void manySmallInHeapWithMapMaker()
    {
        final byte payload[] = new byte[450];
        int ops = many;

        logger.info( "many=" + ops );
        logger.info( "payload.length=" + payload.length );
        pumpTheHeap( ops, payload );

    }

    @Test
    public void manySmallOffHeap()
    {

        final byte payload[] = new byte[450];
        int ops = many;

        logger.info( "many=" + ops );
        logger.info( "payload.length=" + payload.length );
        pumpOffHeap( ops, payload );

    }

    @Test
    public void lessButLargerOffHeap()
    {

        final byte payload[] = new byte[5120];
        int ops = less;

        logger.info( "less=" + ops );
        logger.info( "payload.length=" + payload.length );
        pumpOffHeap( ops, payload );

    }

    @Test
    public void lessButLargerInHeap()
    {

        final byte payload[] = new byte[5120];
        int ops = less;

        logger.info( "less=" + ops );
        logger.info( "payload.length=" + payload.length );
        pumpTheHeap( ops, payload );

    }

    /*
     * ExecutorService executor = Executors.newCachedThreadPool(); Callable<Object> task = new Callable<Object>() {
     * public Object call() { return something.blockingMethod(); } } Future<Object> future = executor.submit(task); try
     * { Object result = future.get(5, TimeUnit.SECONDS); } catch (TimeoutException ex) { // handle the timeout }
     * finally { future.cancel(); // may or may not desire this }
     */

    private void pumpOffHeap( int ops, byte[] payload )
    {

        ConcurrentMap<String, ByteBuffer> test = new MapMaker().concurrencyLevel( 4 ).makeMap();

        logger.info( Ram.inMb( ops * payload.length ) + " in " + ops + " slices to store" );

        ByteBuffer bulk = ByteBuffer.allocateDirect( ops * payload.length );

        double started = System.currentTimeMillis();

        for ( int i = 0; i < ops; i++ )
        {
            bulk.position( i * payload.length );
            final ByteBuffer buf = bulk.duplicate();
            buf.put( payload );
            test.put( "test-" + i, buf );
        }

        double finished = System.currentTimeMillis();

        logger.info( "done in " + ( finished - started ) / 1000 + " seconds" );

        for ( ByteBuffer buf : test.values() )
        {
            buf.clear();
        }
    }

    private void pumpTheHeap( int ops, byte[] payload )
    {

        ConcurrentMap<String, byte[]> test = new MapMaker().concurrencyLevel( 4 ).makeMap();

        logger.info( Ram.inMb( ops * payload.length ) + " in " + ops + " slices to store" );

        double started = System.currentTimeMillis();

        for ( int i = 0; i < ops; i++ )
        {
            test.put( "test-" + i, payload.clone() );
        }

        double finished = System.currentTimeMillis();

        logger.info( "done in " + ( finished - started ) / 1000 + " seconds" );
    }

}
