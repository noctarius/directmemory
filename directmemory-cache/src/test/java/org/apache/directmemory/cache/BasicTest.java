package org.apache.directmemory.cache;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.apache.directmemory.DirectMemory;
import org.apache.directmemory.memory.Pointer;
import org.apache.directmemory.memory.unsafe.UnsafeMemoryManager;
import org.junit.Test;

public class BasicTest
{
    @Test
    public void putRetrieveAndUpdate()
        throws IOException
    {
        CacheService<String, Long> cache =
            new DirectMemory<String, Long>().setNumberOfBuffers( 10 ).setSize( 1000 ).setInitialCapacity( 10000 ).setConcurrencyLevel( 4 ).newCacheService();

        assertNull( cache.retrieve( "a" ) );
        assertNotNull( cache.put( "a", 3L ) );
        assertNotNull( cache.retrieve( "a" ) );
        assertEquals( 3L, cache.retrieve( "a" ).longValue() );

        Pointer<Long> ptr = cache.put( "a", 5L );
        assertNotNull( ptr );
        assertFalse( ptr.isExpired() );
        assertFalse( ptr.isFree() );
        assertNotNull( "pointer should not be null", cache.retrieve( "a" ) );
        assertEquals( 5L, cache.retrieve( "a" ).longValue() );

        cache.close();
    }

    @Test
    public void putRetrieveAndUpdateWithUnsafe()
        throws IOException
    {
        UnsafeMemoryManager<Long> umm = new UnsafeMemoryManager<Long>();
        umm.init( 10, 10000 );
        CacheService<String, Long> cache =
            new DirectMemory<String, Long>().setSize( 1000 ).setConcurrencyLevel( 4 ).setMemoryManager( umm ).newCacheService();

        assertNull( cache.retrieve( "a" ) );
        assertNotNull( cache.put( "a", 3L ) );
        assertNotNull( cache.retrieve( "a" ) );
        assertEquals( 3L, cache.retrieve( "a" ).longValue() );

        Pointer<Long> ptr = cache.put( "a", 5L );
        assertNotNull( ptr );
        assertFalse( ptr.isExpired() );
        assertFalse( ptr.isFree() );
        assertNotNull( "pointer should not be null", cache.retrieve( "a" ) );
        assertEquals( 5L, cache.retrieve( "a" ).longValue() );

        cache.close();
    }

}
