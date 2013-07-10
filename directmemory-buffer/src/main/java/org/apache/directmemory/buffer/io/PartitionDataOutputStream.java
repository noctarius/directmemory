package org.apache.directmemory.buffer.io;

import java.io.DataOutputStream;

import org.apache.directmemory.buffer.PartitionBuffer;


public class PartitionDataOutputStream
    extends DataOutputStream
{

    public PartitionDataOutputStream( PartitionBuffer partitionBuffer )
    {
        super( new PartitionOutputStream( partitionBuffer ) );
    }

}
