package org.apache.directmemory.buffer.selector;

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
