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
import java.util.ArrayList;
import java.util.List;

import org.apache.directmemory.buffer.PartitionBuffer;
import org.junit.Test;

public class SlabByteBufferAllocatorTest
{
    @Test
    public void allocationTest()
        throws IOException
    {

        List<FixedSizeByteBufferAllocator> slabs = new ArrayList<FixedSizeByteBufferAllocator>();
        slabs.add( new FixedSizeByteBufferAllocator( 0, 1024, 128, 1 ) );
        slabs.add( new FixedSizeByteBufferAllocator( 1, 1024, 256, 1 ) );
        slabs.add( new FixedSizeByteBufferAllocator( 2, 1024, 512, 1 ) );
        slabs.add( new FixedSizeByteBufferAllocator( 3, 1024, 1024, 1 ) );

        Allocator allocator = new SlabByteBufferAllocator( 0, slabs, false );

        PartitionBuffer bf1 = allocator.allocate( 250 );
        assertEquals( 256, bf1.maxCapacity() );
        assertEquals( 250, bf1.capacity() );

        PartitionBuffer bf2 = allocator.allocate( 251 );
        assertEquals( 256, bf2.maxCapacity() );
        assertEquals( 251, bf2.capacity() );

        PartitionBuffer bf3 = allocator.allocate( 200 );
        assertEquals( 256, bf3.maxCapacity() );
        assertEquals( 200, bf3.capacity() );

        PartitionBuffer bf4 = allocator.allocate( 100 );
        assertEquals( 128, bf4.maxCapacity() );
        assertEquals( 100, bf4.capacity() );

        PartitionBuffer bf5 = allocator.allocate( 550 );
        assertEquals( 1024, bf5.maxCapacity() );
        assertEquals( 550, bf5.capacity() );

        PartitionBuffer bf6 = allocator.allocate( 800 );
        assertNull( bf6 );

        allocator.free( bf5 );

        PartitionBuffer bf7 = allocator.allocate( 800 );
        assertEquals( 1024, bf7.maxCapacity() );
        assertEquals( 800, bf7.capacity() );

        allocator.close();
    }

}
