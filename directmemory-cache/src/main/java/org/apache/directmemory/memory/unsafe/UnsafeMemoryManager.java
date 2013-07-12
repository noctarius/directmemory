package org.apache.directmemory.memory.unsafe;

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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.directmemory.buffer.PartitionBuffer;
import org.apache.directmemory.memory.AbstractMemoryManager;
import org.apache.directmemory.memory.MemoryManager;
import org.apache.directmemory.memory.MemoryManagerHelper;
import org.apache.directmemory.memory.Pointer;
import org.apache.directmemory.memory.PointerImpl;
import org.apache.directmemory.memory.allocator.Allocator;
import org.apache.directmemory.memory.allocator.LazyUnsafeAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnsafeMemoryManager<V>
    extends AbstractMemoryManager<V>
    implements MemoryManager<V>
{

    protected static final long NEVER_EXPIRES = 0L;

    protected static Logger logger = LoggerFactory.getLogger( MemoryManagerHelper.class );

    private final Set<Pointer<V>> pointers = Collections.newSetFromMap( new ConcurrentHashMap<Pointer<V>, Boolean>() );

    private Allocator allocator;

    private long capacity;

    /**
     * Initialize the internal structure. Need to be called before the service can be used.
     * 
     * @param numberOfBuffers : number of internal bucket
     * @param size : size in B of internal buckets
     */
    public void init( int numberOfBuffers, int size )
    {
        this.capacity = numberOfBuffers * size;
        this.allocator = new LazyUnsafeAllocator( numberOfBuffers, capacity );
    }

    @Override
    public void close()
        throws IOException
    {
        allocator.close();
        used.set( 0 );
    }

    protected Pointer<V> instanciatePointer( int size, long expiresIn, long expires )
    {
        Pointer<V> p = new PointerImpl<V>( allocator.allocate( size ), 1 );

        p.setExpiration( expires, expiresIn );
        p.setFree( false );
        p.createdNow();

        pointers.add( p );

        return p;
    }

    @Override
    public Pointer<V> store( byte[] payload, long expiresIn )
    {
        if ( capacity - used.get() - payload.length < 0 )
        {
            if ( returnsNullWhenFull() )
            {
                return null;
            }
            else
            {
                throw new BufferOverflowException();
            }
        }

        Pointer<V> p = instanciatePointer( payload.length, expiresIn, NEVER_EXPIRES );
        p.getPartitionBuffer().writeBytes( payload );

        used.addAndGet( payload.length );
        // 2nd version
        // unsafe.copyMemory( srcAddress, address, payload.length );
        return p;
    }

    @Override
    public byte[] retrieve( Pointer<V> pointer )
    {
        final byte[] swp = new byte[(int) pointer.getSize()];

        PartitionBuffer memoryBuffer = pointer.getPartitionBuffer();
        memoryBuffer.readerIndex( 0 );
        memoryBuffer.readBytes( swp );

        return swp;
    }

    @Override
    public Pointer<V> free( Pointer<V> pointer )
    {
        used.addAndGet( -pointer.getSize() );
        allocator.free( pointer.getPartitionBuffer() );
        pointers.remove( pointer );
        pointer.setFree( true );
        return pointer;
    }

    @Override
    public void clear()
    {
        for ( Pointer<V> pointer : pointers )
        {
            free( pointer );
        }
    }

    @Override
    public long capacity()
    {
        return capacity;
    }

    @Override
    public long used()
    {
        return used.get();
    }

}
