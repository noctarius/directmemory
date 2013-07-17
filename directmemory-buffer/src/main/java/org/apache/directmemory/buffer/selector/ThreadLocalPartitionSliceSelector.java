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

import org.apache.directmemory.buffer.BufferUnderflowException;
import org.apache.directmemory.buffer.spi.Partition;
import org.apache.directmemory.buffer.spi.PartitionSlice;
import org.apache.directmemory.buffer.spi.PartitionSliceSelector;

public class ThreadLocalPartitionSliceSelector
    implements PartitionSliceSelector
{

    private static final boolean ENABLE_TLA_STATISTICS = Boolean.getBoolean( "directmemory.buffer.stats.tla.enabled" );

    private final ThreadLocal<Partition> partitionAssignment = new ThreadLocal<Partition>();

    private final AccessStatistics accessStatistics;

    private volatile boolean[] assigned;

    public ThreadLocalPartitionSliceSelector()
    {
        accessStatistics = ENABLE_TLA_STATISTICS ? new AccessStatistics() : null;
    }

    @Override
    public PartitionSlice selectPartitionSlice( Partition[] partitions )
    {
        if ( ENABLE_TLA_STATISTICS )
        {
            accessStatistics.access++;
        }
        Partition partition = partitionAssignment.get();
        if ( partition != null && partition.available() > 0 )
        {
            PartitionSlice slice = partition.get();
            if ( slice != null )
            {
                return slice;
            }
        }

        synchronized ( this )
        {
            if ( assigned == null )
            {
                assigned = new boolean[partitions.length];
            }

            for ( int index = 0; index < assigned.length; index++ )
            {
                if ( !assigned[index] )
                {
                    assigned[index] = true;
                    partition = partitions[index];
                    if ( partition.available() > 0 )
                    {
                        partitionAssignment.set( partition );
                        PartitionSlice slice = partition.get();
                        if ( slice != null )
                        {
                            if ( ENABLE_TLA_STATISTICS )
                            {
                                accessStatistics.reallocatePartition++;
                            }
                            return slice;
                        }
                    }
                }
            }

            for ( int index = 0; index < partitions.length; index++ )
            {
                if ( partitions[index].available() > 0 )
                {
                    PartitionSlice slice = partitions[index].get();
                    if ( slice != null )
                    {
                        if ( ENABLE_TLA_STATISTICS )
                        {
                            accessStatistics.collisions++;
                        }
                        return slice;
                    }
                }
            }
        }

        throw new BufferUnderflowException( "Could not retrieve a new partition slice" );
    }

    @Override
    public void freePartitionSlice( Partition partition, int partitionIndex, PartitionSlice slice )
    {
        if ( partition.available() == 0 )
        {
            assigned[partitionIndex] = false;
            if ( ENABLE_TLA_STATISTICS )
            {
                System.out.println( accessStatistics.toString() );
            }
        }
    }

    private static class AccessStatistics
    {

        private volatile long access = 0;

        private volatile long collisions = 0;

        private volatile long reallocatePartition = 0;

        @Override
        public String toString()
        {
            return "TLA-AccessStatistics [access=" + access + ", collisions=" + collisions + ", reallocatePartition="
                + reallocatePartition + "]";
        }
    }

}
