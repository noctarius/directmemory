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

import java.nio.ByteOrder;

import org.apache.directmemory.buffer.impl.AbstractPartitionBuffer;
import org.apache.directmemory.buffer.impl.BufferUtils;
import org.apache.directmemory.memory.IllegalMemoryPointerException;

public class UnsafePartitionBuffer
    extends AbstractPartitionBuffer
{

    private static final sun.misc.Unsafe UNSAFE = BufferUtils.getUnsafe();

    final long baseAddress;

    final long capacity;

    private ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;

    UnsafePartitionBuffer( long baseAddress, long capacity )
    {
        if ( baseAddress == 0 )
        {
            throw new IllegalMemoryPointerException( "The pointers base address is not legal" );
        }

        this.baseAddress = baseAddress;
        this.capacity = capacity;
    }

    @Override
    public long capacity()
    {
        return writerIndex();
    }

    @Override
    public long maxCapacity()
    {
        return capacity;
    }

    @Override
    public boolean growing()
    {
        return false;
    }

    @Override
    public ByteOrder byteOrder()
    {
        return byteOrder;
    }

    @Override
    public void byteOrder( ByteOrder byteOrder )
    {
        this.byteOrder = byteOrder;
    }

    @Override
    public void free()
    {
        UNSAFE.freeMemory( baseAddress );
    }

    @Override
    public void clear()
    {
        UNSAFE.setMemory( baseAddress, capacity, (byte) 0 );
        writerIndex = 0;
        readerIndex = 0;
    }

    @Override
    public int sliceByteSize()
    {
        if ( capacity > Integer.MAX_VALUE )
        {
            return Integer.MAX_VALUE;
        }
        return (int) capacity;
    }

    @Override
    public int slices()
    {
        return 1;
    }

    @Override
    public int readBytes( byte[] bytes, int offset, int length )
    {
        long readableBytes = Math.min( length, capacity - readerIndex );
        UNSAFE.copyMemory( null, baseAddress + readerIndex, bytes, BufferUtils.BYTE_ARRAY_OFFSET, readableBytes );
        return (int) readableBytes;
    }

    @Override
    public void writeBytes( byte[] bytes, int offset, int length )
    {
        if ( offset == writerIndex )
        {
            flush();
        }
        long writableBytes = capacity - writerIndex;
        if ( length > writableBytes )
        {
            throw new IllegalStateException( "Allocated memory area is too small to write to!" );
        }
        UNSAFE.copyMemory( buffer, BufferUtils.BYTE_ARRAY_OFFSET, null, baseAddress + writerIndex, length );
    }

    @Override
    public void flush()
    {
        if ( bufferPos > 0 )
        {
            long writableBytes = capacity - writerIndex - bufferPos;
            if ( bufferPos > writableBytes )
            {
                throw new IllegalStateException( "Allocated memory area is too small to write to!" );
            }

            UNSAFE.copyMemory( buffer, BufferUtils.BYTE_ARRAY_OFFSET, null, baseAddress + writerIndex - bufferPos,
                               bufferPos );
            bufferPos = 0;
        }
    }

    @Override
    protected void put( long offset, byte value )
    {
        UNSAFE.putByte( baseAddress + offset, value );
    }

    @Override
    protected byte read( long offset )
    {
        return UNSAFE.getByte( baseAddress + offset );
    }

    @Override
    public short readShort()
    {
        short value = UNSAFE.getShort( baseAddress + readerIndex );
        readerIndex += 2;
        return value;
    }

    @Override
    public char readChar()
    {
        char value = UNSAFE.getChar( baseAddress + readerIndex );
        readerIndex += 2;
        return value;
    }

    @Override
    public int readInt()
    {
        int value = UNSAFE.getInt( baseAddress + readerIndex );
        readerIndex += 4;
        return value;
    }

    @Override
    public long readLong()
    {
        long value = UNSAFE.getLong( baseAddress + readerIndex );
        readerIndex += 8;
        return value;
    }

    @Override
    public float readFloat()
    {
        float value = UNSAFE.getFloat( baseAddress + readerIndex );
        readerIndex += 4;
        return value;
    }

    @Override
    public double readDouble()
    {
        double value = UNSAFE.getDouble( baseAddress + readerIndex );
        readerIndex += 8;
        return value;
    }

    @Override
    public void writeShort( short value )
    {
        UNSAFE.putShort( baseAddress + writerIndex, value );
        writerIndex += 2;
    }

    @Override
    public void writeChar( int value )
    {
        UNSAFE.putChar( baseAddress + writerIndex, (char) value );
        writerIndex += 2;
    }

    @Override
    public void writeInt( int value )
    {
        UNSAFE.putInt( baseAddress + writerIndex, value );
        writerIndex += 4;
    }

    @Override
    public void writeLong( long value )
    {
        UNSAFE.putLong( baseAddress + writerIndex, value );
        writerIndex += 8;
    }

    @Override
    public void writeFloat( float value )
    {
        UNSAFE.putFloat( baseAddress + writerIndex, value );
        writerIndex += 4;
    }

    @Override
    public void writeDouble( double value )
    {
        UNSAFE.putDouble( baseAddress + writerIndex, value );
        writerIndex += 8;
    }

}
