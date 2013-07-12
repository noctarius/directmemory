package org.apache.directmemory.memory.allocator;

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
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.apache.directmemory.buffer.PartitionBuffer;
import org.junit.Test;

public class FixedSizeByteBufferAllocatorImplTest
{
    @Test
    public void allocationTest()
        throws IOException
    {

        Allocator allocator = new FixedSizeByteBufferAllocator( 0, 5000, 256, 1 );

        PartitionBuffer bf1 = allocator.allocate( 250 );
        assertEquals( 256, bf1.maxCapacity() );
        assertEquals( 250, bf1.capacity() );

        PartitionBuffer bf2 = allocator.allocate( 251 );
        assertEquals( 256, bf2.maxCapacity() );
        assertEquals( 251, bf2.capacity() );

        PartitionBuffer bf3 = allocator.allocate( 200 );
        assertEquals( 256, bf3.maxCapacity() );
        assertEquals( 200, bf3.capacity() );

        PartitionBuffer bf4 = allocator.allocate( 2000 );
        assertNull( bf4 );

        PartitionBuffer bf5 = allocator.allocate( 298 );
        assertNull( bf5 );

        PartitionBuffer bf6 = allocator.allocate( 128 );
        assertEquals( 256, bf6.maxCapacity() );
        assertEquals( 128, bf6.capacity() );

        allocator.close();
    }

    @Test
    public void releaseTest()
        throws IOException
    {

        Allocator allocator = new FixedSizeByteBufferAllocator( 0, 1000, 256, 1 );

        PartitionBuffer bf1 = allocator.allocate( 250 );
        assertEquals( 256, bf1.maxCapacity() );
        assertEquals( 250, bf1.capacity() );

        PartitionBuffer bf2 = allocator.allocate( 251 );
        assertEquals( 256, bf2.maxCapacity() );
        assertEquals( 251, bf2.capacity() );

        PartitionBuffer bf3 = allocator.allocate( 252 );
        assertEquals( 256, bf3.maxCapacity() );
        assertEquals( 252, bf3.capacity() );

        PartitionBuffer bf4 = allocator.allocate( 500 );
        assertNull( bf4 );

        allocator.free( bf1 );
        allocator.free( bf2 );

        PartitionBuffer bf5 = allocator.allocate( 500 );
        assertNull( bf5 );

        PartitionBuffer bf6 = allocator.allocate( 249 );
        assertEquals( 256, bf6.maxCapacity() );
        assertEquals( 249, bf6.capacity() );

        PartitionBuffer bf7 = allocator.allocate( 248 );
        assertEquals( 256, bf7.maxCapacity() );
        assertEquals( 248, bf7.capacity() );

        allocator.close();
    }

    @Test
    public void allocateAndFreeTest()
        throws IOException
    {

        Allocator allocator = new FixedSizeByteBufferAllocator( 0, 1000, 256, 1 );

        for ( int i = 0; i < 1000; i++ )
        {
            PartitionBuffer bf1 = allocator.allocate( 250 );
            assertEquals( 256, bf1.maxCapacity() );
            assertEquals( 250, bf1.capacity() );

            allocator.free( bf1 );
        }

        PartitionBuffer bf2 = allocator.allocate( 1000 );
        assertNull( bf2 );

        for ( int i = 0; i < 3; i++ )
        {
            PartitionBuffer bf3 = allocator.allocate( 250 );
            assertEquals( 256, bf3.maxCapacity() );
            assertEquals( 250, bf3.capacity() );

        }

        PartitionBuffer bf4 = allocator.allocate( 238 );
        assertNull( bf4 );

        allocator.close();
    }

}
