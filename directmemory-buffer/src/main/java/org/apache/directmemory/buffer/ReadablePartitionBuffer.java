package org.apache.directmemory.buffer;

public interface ReadablePartitionBuffer
{

    boolean readable();

    long readableSize();

    byte readByte();

    short readUnsignedByte();

    int readBytes( byte[] bytes );

    int readBytes( byte[] bytes, int offset, int length );

    boolean readBoolean();

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

    String readString();

    int readCompressedInt();

    long readUnsignedInt();

    long readerIndex();

    void readerIndex( long readerIndex );

}
