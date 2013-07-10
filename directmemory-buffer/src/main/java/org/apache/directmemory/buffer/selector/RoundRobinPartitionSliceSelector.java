package org.apache.directmemory.buffer.selector;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.directmemory.buffer.spi.Partition;
import org.apache.directmemory.buffer.spi.PartitionSlice;
import org.apache.directmemory.buffer.spi.PartitionSliceSelector;


public class RoundRobinPartitionSliceSelector
    implements PartitionSliceSelector
{

    private final Lock lock = new ReentrantLock();

    private volatile int index = 0;

    @Override
    public PartitionSlice selectPartitionSlice( Partition[] partitions )
    {
        lock.lock();
        try
        {
            int retry = 0;
            while ( retry < partitions.length )
            {
                Partition partition = partitions[index++];
                if ( index == partitions.length )
                {
                    index = 0;
                }
                if ( partition.available() > 0 )
                {
                    return partition.get();
                }
                retry++;
            }
        }
        finally
        {
            lock.unlock();
        }

        throw new RuntimeException( "Could not retrieve a new partition slice" );
    }

    @Override
    public void freePartitionSlice( Partition partition, int partitionIndex, PartitionSlice slice )
    {
    }

}
