package org.apache.directmemory.buffer.io;

import java.io.DataInputStream;

import org.apache.directmemory.buffer.PartitionBuffer;


public class PartitionDataInputStream
    extends DataInputStream
{

    public PartitionDataInputStream( PartitionBuffer partitionBuffer )
    {
        super( new PartitionInputStream( partitionBuffer ) );
    }

}
