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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.directmemory.buffer.PartitionBuffer;
import org.apache.directmemory.buffer.ReadablePartitionBuffer;
import org.apache.directmemory.buffer.utils.BufferUtils;
import org.apache.directmemory.buffer.utils.Int32Compressor;
import org.apache.directmemory.buffer.utils.Int64Compressor;
import org.apache.directmemory.buffer.utils.UnicodeUtils;

public abstract class AbstractPartitionBuffer
    implements PartitionBuffer
{

    protected final byte[] buffer = new byte[1024];

    protected volatile int bufferPos = 0;

    protected ByteOrder byteOrder;

    protected volatile long writerIndex = 0;

    protected volatile long readerIndex = 0;

    @Override
    public void close()
    {
        free();
    }

    @Override
    public boolean readable()
    {
        return writerIndex - readerIndex > 0;
    }

    @Override
    public long readableSize()
    {
        return writerIndex;
    }

    @Override
    public boolean readBoolean()
    {
        return read() == 1;
    }

    @Override
    public byte readByte()
    {
        return read();
    }

    @Override
    public short readUnsignedByte()
    {
        return (short) ( read() & 0xFF );
    }

    @Override
    public int readBytes( byte[] bytes )
    {
        int read = readBytes( bytes, 0, bytes.length );
        readerIndex += read;
        return read;
    }

    @Override
    public char readChar()
    {
        return (char) readShort();
    }

    @Override
    public double readDouble()
    {
        return Double.longBitsToDouble( readLong() );
    }

    @Override
    public double readCompressedDouble()
    {
        return Double.longBitsToDouble( readCompressedLong() );
    }

    @Override
    public float readFloat()
    {
        return Float.intBitsToFloat( readInt() );
    }

    @Override
    public float readCompressedFloat()
    {
        return Float.intBitsToFloat( readCompressedInt() );
    }

    @Override
    public long readLong()
    {
        return BufferUtils.getLong( this, byteOrder == ByteOrder.BIG_ENDIAN );
    }

    @Override
    public long readCompressedLong()
    {
        return Int64Compressor.readInt64( this );
    }

    @Override
    public short readShort()
    {
        return BufferUtils.getShort( this, byteOrder == ByteOrder.BIG_ENDIAN );
    }

    @Override
    public int readUnsignedShort()
    {
        return readShort() & 0xFFFF;
    }

    @Override
    public int readInt()
    {
        return BufferUtils.getInt( this, byteOrder == ByteOrder.BIG_ENDIAN );
    }

    @Override
    public int readCompressedInt()
    {
        return Int32Compressor.readInt32( this );
    }

    @Override
    public long readUnsignedInt()
    {
        return readInt() & 0xFFFFFFFFL;
    }

    @Override
    public String readString()
    {
        return UnicodeUtils.UTF8toUTF16( this );
    }

    @Override
    public long readerIndex()
    {
        return readerIndex;
    }

    @Override
    public void readerIndex( long readerIndex )
    {
        this.readerIndex = readerIndex;
    }

    @Override
    public boolean writeable()
    {
        return maxCapacity() - writerIndex > 0;
    }

    @Override
    public void writeBoolean( boolean value )
    {
        put( (byte) ( value ? 1 : 0 ) );
    }

    @Override
    public void writeByte( int value )
    {
        put( (byte) value );
    }

    @Override
    public void writeBytes( byte[] bytes )
    {
        writeBytes( bytes, 0, bytes.length );
        writerIndex += bytes.length;
    }

    @Override
    public void writeByteBuffer( ByteBuffer byteBuffer )
    {
        byteBuffer.mark();
        byteBuffer.position( 0 );

        if ( byteBuffer.hasArray() )
        {
            writeBytes( byteBuffer.array(), byteBuffer.arrayOffset(), byteBuffer.remaining() );
        }
        else
        {
            while ( byteBuffer.hasRemaining() )
            {
                put( byteBuffer.get() );
            }
        }
        byteBuffer.reset();
    }

    @Override
    public void writeByteBuffer( ByteBuffer byteBuffer, int offset, int length )
    {
        byteBuffer.mark();
        byteBuffer.position( offset );
        if ( byteBuffer.hasArray() )
        {
            writeBytes( byteBuffer.array(), byteBuffer.arrayOffset() + offset, length );
        }
        else
        {
            int position = 0;
            while ( position++ < length )
            {
                put( byteBuffer.get() );
            }
        }
        byteBuffer.reset();
    }

    @Override
    public void writePartitionBuffer( ReadablePartitionBuffer partitionBuffer )
    {
        writePartitionBuffer( partitionBuffer, 0, partitionBuffer.readableSize() );
    }

    @Override
    public void writePartitionBuffer( ReadablePartitionBuffer partitionBuffer, long offset, long length )
    {
        long readerIndex = partitionBuffer.readerIndex();
        partitionBuffer.readerIndex( offset );
        long position = 0;
        while ( position++ < length )
        {
            put( partitionBuffer.readByte() );
        }
        partitionBuffer.readerIndex( readerIndex );
    }

    @Override
    public void writeChar( int value )
    {
        writeShort( (short) value );
    }

    @Override
    public void writeDouble( double value )
    {
        writeLong( Double.doubleToLongBits( value ) );
    }

    @Override
    public void writeCompressedDouble( double value )
    {
        writeCompressedLong( Double.doubleToLongBits( value ) );
    }

    @Override
    public void writeFloat( float value )
    {
        writeInt( Float.floatToIntBits( value ) );
    }

    @Override
    public void writeCompressedFloat( float value )
    {
        writeCompressedInt( Float.floatToIntBits( value ) );
    }

    @Override
    public void writeLong( long value )
    {
        BufferUtils.putLong( value, this, byteOrder == ByteOrder.BIG_ENDIAN );
    }

    @Override
    public void writeCompressedLong( long value )
    {
        Int64Compressor.writeInt64( value, this );
    }

    @Override
    public void writeShort( short value )
    {
        BufferUtils.putShort( value, this, byteOrder == ByteOrder.BIG_ENDIAN );
    }

    @Override
    public void writeInt( int value )
    {
        BufferUtils.putInt( value, this, byteOrder == ByteOrder.BIG_ENDIAN );
    }

    @Override
    public void writeCompressedInt( int value )
    {
        Int32Compressor.writeInt32( value, this );
    }

    @Override
    public void writeString( String value )
    {
        UnicodeUtils.UTF16toUTF8( value, this );
    }

    @Override
    public long writerIndex()
    {
        return writerIndex;
    }

    @Override
    public void writerIndex( long writerIndex )
    {
        this.writerIndex = writerIndex;
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

    protected byte read()
    {
        return read( readerIndex++ );
    }

    protected void put( byte value )
    {
        put( writerIndex++, value );
    }

    protected abstract byte read( long position );

    protected abstract void put( long position, byte value );

}
