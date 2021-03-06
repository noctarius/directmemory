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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;


public abstract class AbstractByteBufferAllocator
    implements Allocator
{

    protected final Logger logger = LoggerFactory.getLogger( this.getClass() );

    private final int number;

    private final AtomicBoolean closed = new AtomicBoolean( false );

    AbstractByteBufferAllocator( final int number )
    {
        this.number = number;
    }

    @Override
    public int getNumber()
    {
        return number;
    }

    protected final Logger getLogger()
    {
        return logger;
    }

    protected final boolean isClosed()
    {
        return closed.get();
    }

    protected final void setClosed( final boolean closed )
    {
        this.closed.set( closed );
    }

    protected static Integer getHash( final ByteBuffer buffer )
    {
        return Integer.valueOf( System.identityHashCode( buffer ) );
    }

}
