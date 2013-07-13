package org.apache.directmemory.buffer.impl;

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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.directmemory.buffer.spi.PartitionSlice;

public abstract class AbstractPartitionSlice
    implements PartitionSlice
{

    private final AtomicBoolean lock = new AtomicBoolean( false );

    private final AtomicInteger aquired = new AtomicInteger( 0 );

    private final AtomicInteger freed = new AtomicInteger( 0 );

    final int index;

    AbstractPartitionSlice( int index )
    {
        this.index = index;
    }

    protected synchronized PartitionSlice lock()
    {
        if ( !lock.compareAndSet( false, true ) )
        {
            throw new IllegalStateException( "PartitionSlice already locked" );
        }
        if ( aquired.get() != freed.get() )
        {
            throw new IllegalStateException( "Not all aquires (" + aquired.get() + ") are freed (" + freed.get() + ")" );
        }
        aquired.incrementAndGet();
        return this;
    }

    protected synchronized PartitionSlice unlock()
    {
        if ( !lock.compareAndSet( true, false ) )
        {
            throw new IllegalStateException( "PartitionSlice not locked" );
        }
        freed.incrementAndGet();
        return this;
    }

    protected abstract void free();

}
