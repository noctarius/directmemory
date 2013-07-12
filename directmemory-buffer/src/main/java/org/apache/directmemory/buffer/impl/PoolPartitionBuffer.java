package org.apache.directmemory.buffer.impl;

import java.nio.ByteOrder;
import java.util.Arrays;

import org.apache.directmemory.buffer.spi.PartitionSlice;

class PoolPartitionBuffer
    extends AbstractPartitionBuffer
{

    private final PartitionBufferPoolImpl partitionBufferPool;

    private volatile PartitionSlice[] slices = new PartitionSlice[0];

    public PoolPartitionBuffer( PartitionBufferPoolImpl partitionBufferPool, ByteOrder byteOrder )
    {
        this.partitionBufferPool = partitionBufferPool;
        this.byteOrder = byteOrder;
        resize( 1 );
    }

    @Override
    public boolean growing()
    {
        return true;
    }

    @Override
    public void clear()
    {
        writerIndex = 0;
        readerIndex = 0;
        if ( slices.length > 1 )
        {
            // Release unused slices
            for ( int i = 1; i < slices.length; i++ )
            {
                partitionBufferPool.freeSlice( slices[i] );
            }
            PartitionSlice firstSlice = slices[0];
            slices = new PartitionSlice[1];
            slices[0] = firstSlice;
        }
        slices[0].clear();
    }

    @Override
    public int readBytes( byte[] bytes, int offset, int length )
    {
        length = Math.min( length, bytes.length - offset );
        int baseSliceIndex = sliceIndex( readerIndex );
        PartitionSlice slice = slices[baseSliceIndex];
        if ( slice.readableBytes() >= length )
        {
            int relativePosition = (int) ( baseSliceIndex == 0 ? readerIndex : readerIndex % sliceByteSize() );
            slice.read( relativePosition, bytes, offset, length );
        }
        else
        {
            int remaining = length - slice.readableBytes();
            int additionalSlices = ( remaining / sliceByteSize() ) + ( remaining % sliceByteSize() != 0 ? 1 : 0 );

            if ( baseSliceIndex + additionalSlices + 1 > slices.length )
            {
                throw new IndexOutOfBoundsException( "Not enough data to load" );
            }

            int sliceOffset = offset;
            for ( int i = baseSliceIndex; i <= baseSliceIndex + additionalSlices; i++ )
            {
                int readable = Math.min( slices[i].readableBytes(), length - sliceOffset );
                slices[i].read( bytes, sliceOffset, readable );
                sliceOffset += readable;
            }
        }
        readerIndex += length;
        return length;
    }

    @Override
    public void writeBytes( byte[] bytes, int offset, int length )
    {
        if ( offset == writerIndex )
        {
            flush();
        }
        int baseSliceIndex = slices.length - 1;
        PartitionSlice slice = slices[baseSliceIndex];
        if ( slice.writeableBytes() >= length )
        {
            slice.put( bytes, offset, length );
        }
        else
        {
            int remaining = length - slice.writeableBytes();
            int additionalSlices = ( remaining / sliceByteSize() ) + ( remaining % sliceByteSize() != 0 ? 1 : 0 );

            resize( slices.length + additionalSlices );

            int sliceOffset = offset;
            for ( int i = baseSliceIndex; i <= baseSliceIndex + additionalSlices; i++ )
            {
                int writeable = Math.min( slices[i].writeableBytes(), length - sliceOffset );
                slices[i].put( bytes, sliceOffset, writeable );
                sliceOffset += writeable;
            }
        }
        writerIndex += length;
    }

    @Override
    public long capacity()
    {
        long capacity = 0;
        for ( PartitionSlice slice : slices )
        {
            capacity += slice != null ? slice.writerIndex() : 0L;
        }
        return capacity;
    }

    @Override
    public long maxCapacity()
    {
        return slices() * sliceByteSize();
    }

    @Override
    public int sliceByteSize()
    {
        return partitionBufferPool.getSliceByteSize();
    }

    @Override
    public int slices()
    {
        int slices = 0;
        for ( int i = 0; i < this.slices.length; i++ )
        {
            slices += this.slices[i] != null ? 1 : 0;
        }
        return slices;
    }

    @Override
    public void free()
    {
        synchronized ( slices )
        {
            for ( PartitionSlice slice : slices )
            {
                partitionBufferPool.freeSlice( slice );
            }
            Arrays.fill( slices, null );
        }
    }

    @Override
    public void flush()
    {
        if ( bufferPos > 0 )
        {
            long position = writerIndex - bufferPos;
            int sliceIndex = sliceIndex( position );
            if ( sliceIndex >= slices.length )
            {
                resize( sliceIndex + 1 );
            }
            int writableBytes = slices[sliceIndex].writeableBytes();
            if ( bufferPos > writableBytes )
            {
                int offset = 0;
                while ( offset < bufferPos )
                {
                    int bytes = Math.min( bufferPos, writableBytes );
                    slices[sliceIndex].put( buffer, offset, bytes );
                    offset += bytes;
                    writableBytes = slices[++sliceIndex].writeableBytes();
                }
            }
            else
            {
                int relativePosition = (int) ( sliceIndex == 0 ? position : position % sliceByteSize() );
                slices[sliceIndex].put( relativePosition, buffer, 0, bufferPos );
            }
            bufferPos = 0;
        }
    }

    @Override
    protected void put( byte value )
    {
        put( writerIndex++, value );
    }

    @Override
    protected void put( long position, byte value )
    {
        if ( bufferPos == buffer.length )
        {
            flush();
        }

        if ( position == writerIndex - 1 )
        {
            buffer[bufferPos++] = value;
            if ( bufferPos == buffer.length )
            {
                flush();
            }
        }
        else
        {
            int sliceIndex = sliceIndex( position );
            if ( sliceIndex >= slices.length )
            {
                resize( sliceIndex + 1 );
            }

            int relativePosition = (int) ( sliceIndex == 0 ? position : position % sliceByteSize() );
            slices[sliceIndex].put( relativePosition, value );
        }
    }

    @Override
    protected byte read()
    {
        return read( readerIndex++ );
    }

    @Override
    protected byte read( long position )
    {
        if ( position > writerIndex )
        {
            throw new IndexOutOfBoundsException( "Position " + position + " is not readable" );
        }
        int sliceIndex = sliceIndex( position );
        return slices[sliceIndex].read( (int) ( sliceIndex == 0 ? position : position % sliceByteSize() ) );
    }

    private int sliceIndex( long position )
    {
        return (int) ( position / sliceByteSize() );
    }

    private synchronized void resize( int newSize )
    {
        int oldSize = slices.length;
        PartitionSlice[] temp = new PartitionSlice[newSize];
        if ( slices != null )
        {
            System.arraycopy( slices, 0, temp, 0, slices.length );
        }
        for ( int i = oldSize; i < newSize; i++ )
        {
            temp[i] = partitionBufferPool.requestSlice();
        }
        if ( temp[temp.length - 1] != null )
        {
            slices = temp;
        }
    }

}
