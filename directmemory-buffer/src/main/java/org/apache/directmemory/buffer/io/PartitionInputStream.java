package org.apache.directmemory.buffer.io;

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
import java.io.InputStream;

import org.apache.directmemory.buffer.PartitionBuffer;

public class PartitionInputStream
    extends InputStream
{

    private final PartitionBuffer partitionBuffer;

    private long markedPosition = 0;

    public PartitionInputStream( PartitionBuffer partitionBuffer )
    {
        this.partitionBuffer = partitionBuffer;
    }

    @Override
    public int read()
        throws IOException
    {
        return partitionBuffer.readByte();
    }

    @Override
    public void close()
        throws IOException
    {
        super.close();
        partitionBuffer.free();
    }

    @Override
    public int read( byte[] bytes )
        throws IOException
    {
        partitionBuffer.readBytes( bytes );
        return bytes.length;
    }

    @Override
    public int read( byte[] bytes, int offset, int length )
        throws IOException
    {
        partitionBuffer.readBytes( bytes, offset, length );
        return length;
    }

    @Override
    public int available()
        throws IOException
    {
        return (int) ( partitionBuffer.writerIndex() - partitionBuffer.readerIndex() );
    }

    @Override
    public synchronized void mark( int readLimit )
    {
        this.markedPosition = partitionBuffer.readerIndex();
    }

    @Override
    public synchronized void reset()
        throws IOException
    {
        partitionBuffer.readerIndex( markedPosition );
    }

    @Override
    public boolean markSupported()
    {
        return true;
    }

}
