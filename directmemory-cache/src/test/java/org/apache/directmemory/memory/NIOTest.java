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

import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.memory.buffer.MemoryBuffer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.*;

@Ignore
public class NIOTest
{

    private static Logger logger = LoggerFactory.getLogger( NIOTest.class );

    @BeforeClass
    public static void init()
    {
        byte[] payload = "012345678901234567890123456789012345678901234567890123456789".getBytes();

        logger.info( "init" );
        MemoryManagerHelper.init( 1, Ram.Mb( 100 ) );

        logger.info( "payload size=" + Ram.inKb( payload.length ) );
        long howMany = ( MemoryManagerHelper.capacity() / payload.length );
        howMany = ( howMany * 50 ) / 100;

        for ( int i = 0; i < howMany; i++ )
        {
            Pointer<Object> p = MemoryManagerHelper.store( payload );
            assertNotNull( p );
        }

        logger.info( "" + howMany + " items stored" );
    }

    @AfterClass
    public static void cleanup()
        throws IOException
    {
        MemoryManagerHelper.close();
    }

    @Test
    public void nioTest()
    {
        Random rnd = new Random();
        int size = rnd.nextInt( 10 ) * (int) MemoryManagerHelper.capacity() / 100;
        logger.info( "payload size=" + Ram.inKb( size ) );
        Pointer<Object> p = MemoryManagerHelper.allocate( size );
        MemoryBuffer b = p.getMemoryBuffer();
        logger.info( "allocated" );
        assertNotNull( p );
        assertNotNull( b );

        // assertTrue( b.isDirect() );
        assertEquals( 0, b.readerIndex() );
        assertEquals( size, b.capacity() );

        byte[] check = MemoryManagerHelper.retrieve( p );

        assertNotNull( check );

        assertEquals( size, p.getCapacity() );
        logger.info( "end" );
    }

}
