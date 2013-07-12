package org.apache.directmemory.buffer;

import java.nio.ByteBuffer;

public interface WritablePartitionBuffer
{

    boolean writeable();

    void writeByte( int value );

    void writeBytes( byte[] bytes );

    void writeBytes( byte[] bytes, int offset, int length );

    void writeByteBuffer( ByteBuffer byteBuffer );

    void writeByteBuffer( ByteBuffer byteBuffer, int offset, int length );

    void writePartitionBuffer( ReadablePartitionBuffer partitionBuffer );

    void writePartitionBuffer( ReadablePartitionBuffer partitionBuffer, long offset, long length );

    void writeBoolean( boolean value );

    void writeChar( int value );

    void writeDouble( double value );

    void writeCompressedDouble( double value );

    void writeFloat( float value );

    void writeCompressedFloat( float value );

    void writeLong( long value );

    void writeCompressedLong( long value );

    void writeShort( short value );

    void writeInt( int value );

    void writeCompressedInt( int value );

    void writeString( String value );

    long writerIndex();

    void writerIndex( long writerIndex );

    void flush();

}