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
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.directmemory.buffer.PartitionBuffer;
import org.apache.directmemory.buffer.impl.BufferUtils;

public class LazyUnsafeAllocator
    implements Allocator
{

    private final sun.misc.Unsafe unsafe = BufferUtils.getUnsafe();

    private final Set<UnsafePartitionBuffer> partitionBuffers =
        Collections.newSetFromMap( new ConcurrentHashMap<UnsafePartitionBuffer, Boolean>() );

    private final AtomicLong used = new AtomicLong( 0 );

    private final int number;

    private final long capacity;

    // Tells if it returns null or throw an BufferOverflowException when the requested size is bigger than the size of
    // the slices
    private final boolean returnNullWhenOversizingSliceSize = true;

    public LazyUnsafeAllocator( int number, long capacity )
    {
        this.number = number;
        this.capacity = capacity;

        if ( unsafe == null )
        {
            throw new IllegalStateException( "This JVM has no sun.misc.Unsafe support, "
                + "please choose another MemoryManagerHelper implementation" );
        }
    }

    @Override
    public void close()
        throws IOException
    {
        clear();
        Iterator<UnsafePartitionBuffer> iterator = partitionBuffers.iterator();
        while ( iterator.hasNext() )
        {
            UnsafePartitionBuffer partitionBuffer = iterator.next();
            partitionBuffer.free();
            iterator.remove();
        }
    }

    @Override
    public void free( PartitionBuffer partitionBuffer )
    {
        used.addAndGet( -partitionBuffer.capacity() );
        partitionBuffer.free();
        partitionBuffers.remove( partitionBuffer );
    }

    @Override
    public PartitionBuffer allocate( int size )
    {
        if ( capacity < used.get() + size )
        {
            throw new BufferOverflowException();
        }

        long baseAddress = unsafe.allocateMemory( size );
        UnsafePartitionBuffer partitionBuffer = new UnsafePartitionBuffer( baseAddress, size );
        partitionBuffers.add( partitionBuffer );
        used.addAndGet( size );
        return partitionBuffer;
    }

    @Override
    public void clear()
    {
        for ( UnsafePartitionBuffer partitionBuffer : partitionBuffers )
        {
            unsafe.setMemory( partitionBuffer.baseAddress, partitionBuffer.capacity, (byte) 0 );
        }
    }

    @Override
    public int getCapacity()
    {
        long capacity = 0;
        for ( UnsafePartitionBuffer partitionBuffer : partitionBuffers )
        {
            capacity += partitionBuffer.capacity;
        }
        return (int) capacity;
    }

    @Override
    public int getNumber()
    {
        return number;
    }

}
