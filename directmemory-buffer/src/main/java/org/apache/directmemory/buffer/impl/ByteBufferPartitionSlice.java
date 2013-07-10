package org.apache.directmemory.buffer.impl;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.directmemory.buffer.spi.Partition;

class ByteBufferPartitionSlice
    extends AbstractPartitionSlice
{

    private final AtomicBoolean freed = new AtomicBoolean( false );

    private final ByteBuffer byteBuffer;

    private final Partition partition;

    private final int sliceByteSize;

    private volatile int writerIndex;

    private volatile int readerIndex;

    ByteBufferPartitionSlice( ByteBuffer byteBuffer, int index, Partition partition, int sliceByteSize )
    {
        super( index );

        this.byteBuffer = byteBuffer;
        this.partition = partition;
        this.sliceByteSize = sliceByteSize;
    }

    @Override
    public void clear()
    {
        byteBuffer.clear();
        writerIndex = 0;
        readerIndex = 0;
    }

    @Override
    public void put( byte value )
    {
        put( writerIndex++, value );
    }

    @Override
    public void put( int position, byte value )
    {
        byteBuffer.put( position, value );
    }

    @Override
    public void put( byte[] array, int offset, int length )
    {
        byteBuffer.put( array, offset, length );
    }

    @Override
    public void put( int position, byte[] array, int offset, int length )
    {
        int oldPosition = byteBuffer.position();
        byteBuffer.position( position );
        byteBuffer.put( array, offset, length );
        byteBuffer.position( oldPosition );
    }

    @Override
    public byte read()
    {
        return read( readerIndex++ );
    }

    @Override
    public byte read( int position )
    {
        return byteBuffer.get( position );
    }

    @Override
    public void read( byte[] array, int offset, int length )
    {
        byteBuffer.get( array, offset, length );
    }

    @Override
    public void read( int position, byte[] array, int offset, int length )
    {
        int oldPosition = byteBuffer.position();
        byteBuffer.position( position );
        byteBuffer.get( array, offset, length );
        byteBuffer.position( oldPosition );
    }

    @Override
    public int getSliceByteSize()
    {
        return sliceByteSize;
    }

    @Override
    public int readableBytes()
    {
        return writerIndex - readerIndex;
    }

    @Override
    public int writeableBytes()
    {
        return sliceByteSize - writerIndex;
    }

    @Override
    public int writerIndex()
    {
        return writerIndex;
    }

    @Override
    public int readerIndex()
    {
        return readerIndex;
    }

    @Override
    public void writerIndex( int writerIndex )
    {
        BufferUtils.rangeCheck( writerIndex, 0, sliceByteSize, "writerIndex" );
        this.writerIndex = writerIndex;
    }

    @Override
    public void readerIndex( int readerIndex )
    {
        BufferUtils.rangeCheck( readerIndex, 0, sliceByteSize, "readerIndex" );
        this.readerIndex = readerIndex;
    }

    @Override
    public Partition getPartition()
    {
        return partition;
    }

    @Override
    protected void free()
    {
        if ( freed.compareAndSet( false, true ) )
        {
            return;
        }
        BufferUtils.cleanByteBuffer( byteBuffer );
    }

}
