package org.apache.directmemory.buffer;

public interface ReadablePartitionBuffer
{

    boolean readable();

    long readableSize();

    byte readByte();

    short readUnsignedByte();

    void readBytes( byte[] bytes );

    void readBytes( byte[] bytes, int offset, int length );

    char readChar();

    double readDouble();

    double readCompressedDouble();

    float readFloat();

    float readCompressedFloat();

    long readLong();

    long readCompressedLong();

    short readShort();

    int readUnsignedShort();

    int readInt();

    int readCompressedInt();

    long readUnsignedInt();

    long readerIndex();

    void readerIndex( long readerIndex );

}
