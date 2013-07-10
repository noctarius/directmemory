package org.apache.directmemory.buffer.io;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.directmemory.buffer.PartitionBuffer;


public class PartitionOutputStream
    extends OutputStream
{

    private final PartitionBuffer partitionBuffer;

    public PartitionOutputStream( PartitionBuffer partitionBuffer )
    {
        this.partitionBuffer = partitionBuffer;
    }

    @Override
    public void write( int b )
        throws IOException
    {
        partitionBuffer.writeByte( b );
    }

    @Override
    public void write( byte[] b )
        throws IOException
    {
        partitionBuffer.writeBytes( b );
    }

    @Override
    public void write( byte[] b, int off, int len )
        throws IOException
    {
        partitionBuffer.writeBytes( b, off, len );
    }

    @Override
    public void close()
        throws IOException
    {
        super.close();
        partitionBuffer.free();
    }

}
