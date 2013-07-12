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
import java.util.Map;
import java.util.Random;

import org.apache.directmemory.measures.Ram;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.google.common.collect.Maps;

public class MemoryManagerTest
    extends AbstractBenchmark
{

    @BeforeClass
    public static void init()
    {
        logger.info( "init" );
        MemoryManagerHelper.init( 1, Ram.Mb( 100 ) );
    }

    @AfterClass
    public static void cleanup()
        throws IOException
    {
        MemoryManagerHelper.close();
    }

    @Test
    public void smokeTest()
    {
        Random rnd = new Random();
        int size = rnd.nextInt( 10 ) * (int) MemoryManagerHelper.capacity() / 100;
        logger.info( "payload size=" + Ram.inKb( size ) );
        Pointer<Object> p = MemoryManagerHelper.store( new byte[size] );
        logger.info( "stored" );
        assertNotNull( p );
        // assertEquals(size,p.end);
        assertEquals( size, p.getCapacity() );
        assertEquals( size, MemoryManagerHelper.getMemoryManager().used() );
        MemoryManagerHelper.free( p );
        assertEquals( 0, MemoryManagerHelper.getMemoryManager().used() );
        logger.info( "end" );
    }

    byte[] payload = "012345678901234567890123456789012345678901234567890123456789".getBytes();

    @Test
    public void fillupTest()
    {
        MemoryManagerHelper.clear();
        logger.info( "payload size=" + Ram.inKb( payload.length ) );
        long howMany = ( MemoryManagerHelper.capacity() / payload.length );
        howMany = ( howMany * 90 ) / 100;

        for ( int i = 0; i < howMany; i++ )
        {
            Pointer<Object> p = MemoryManagerHelper.store( payload );
            assertNotNull( p );
        }

        logger.info( "" + howMany + " items stored" );
    }

    @Test
    public void readTest()
    {
        for ( Pointer<Object> ptr : MemoryManagerHelper.getMemoryManager().getPointers() )
        {
            if ( !ptr.isFree() )
            {
                byte[] res = MemoryManagerHelper.retrieve( ptr );
                assertNotNull( res );
                assertEquals( new String( payload ), new String( res ) );
            }
        }
    }

    private static Logger logger = LoggerFactory.getLogger( MallocTest.class );

    final static Map<String, Byte> test = Maps.newHashMap();

}
