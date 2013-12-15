package org.apache.directmemory.memory;

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
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.memory.unsafe.UnsafeMemoryManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapMaker;

@Ignore
public class MallocWithUnsafeTest
{

    Random rnd = new Random();

    private static Logger logger = LoggerFactory.getLogger( MallocWithUnsafeTest.class );

    @After
    public void dump()
        throws IOException
    {
        logger.info( "off-heap allocated: " + Ram.inMb( mem.capacity() ) );
        logger.info( "off-heap used:      " + Ram.inMb( mem.used() ) );
        logger.info( "heap - max: " + Ram.inMb( Runtime.getRuntime().maxMemory() ) );
        logger.info( "heap - allocated: " + Ram.inMb( Runtime.getRuntime().totalMemory() ) );
        logger.info( "heap - free : " + Ram.inMb( Runtime.getRuntime().freeMemory() ) );
        logger.info( "************************************************" );

        if ( mem != null )
        {
            mem.close();
        }
    }

    MemoryManager<Object> mem;

    @Before
    public void initMMS()
    {
        mem = new UnsafeMemoryManager<Object>();
        mem.init( 1, 512 * 1024 * 1024 );
    }

    @Test
    public void oneMillionEntries()
    {
        assertNotNull( mem );
        int howMany = 1000000;
        int size = (int) mem.capacity() / ( howMany );
        size -= size / 100 * 1;
        logger.info( "payload size=" + size );
        logger.info( "entries=" + howMany );

        logger.info( "starting..." );

        long start = System.currentTimeMillis();

        byte[] payload = new byte[size];
        for ( int i = 0; i < howMany; i++ )
        {
            mem.store( payload );
        }

        logger.info( "...done in " + ( System.currentTimeMillis() - start ) + " msecs." );
    }

    @Test
    public void twoMillionEntries()
    {

        assertNotNull( mem );
        int howMany = 2000000;
        int size = (int) mem.capacity() / ( howMany );
        size -= size / 100 * 1;
        logger.info( "payload size=" + size );
        logger.info( "entries=" + howMany );

        logger.info( "starting..." );
        long start = System.currentTimeMillis();

        byte[] payload = new byte[size];
        for ( int i = 0; i < howMany; i++ )
        {
            mem.store( payload );
        }

        logger.info( "...done in " + ( System.currentTimeMillis() - start ) + " msecs." );
    }

    @Test
    public void fiveMillionEntries()
    {

        assertNotNull( mem );
        int howMany = 5000000;
        int size = (int) mem.capacity() / ( howMany );
        size -= size / 100 * 1;
        logger.info( "payload size=" + size );
        logger.info( "entries=" + howMany );

        logger.info( "starting..." );
        long start = System.currentTimeMillis();

        byte[] payload = new byte[size];
        for ( int i = 0; i < howMany; i++ )
        {
            mem.store( payload );
        }

        logger.info( "...done in " + ( System.currentTimeMillis() - start ) + " msecs." );
    }

    @Test
    public void withMap()
    {

        ConcurrentMap<Long, Pointer<Object>> map =
            new MapMaker().concurrencyLevel( 4 ).initialCapacity( 500000 ).makeMap();

        String str = "This is the string to store into the off-heap memory";

        int size = str.length();
        int howMany = 1000000;
        byte[] payload = str.getBytes();

        logger.info( "adding " + howMany + " strings of " + size + " bytes..." );
        for ( long i = 0; i < howMany; i++ )
        {
            Pointer<Object> p = mem.store( payload );
            map.put( i, p );
        }
        logger.info( "...done" );

    }

    @Before
    public void before()
    {
        mem.clear();
    }

    @Test
    public void oneMillionEntriesWithRead()
    {

        logger.info( "total capacity=" + Ram.inMb( mem.capacity() ) );
        assertNotNull( mem );
        int size = 400;
        int howMany = 1000000;
        logger.info( "payload size=" + Ram.inKb( size ) );
        logger.info( "entries=" + howMany );
        String test = "this is a nicely crafted test";
        byte[] payload = test.getBytes();
        for ( int i = 0; i < howMany; i++ )
        {
            Pointer<Object> p = mem.store( payload );
            byte[] check = mem.retrieve( p );
            assertNotNull( check );
            assertEquals( test, new String( check ) );
        }

        logger.info( "total used=" + Ram.inMb( mem.used() ) );
    }
}
