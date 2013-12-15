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

import com.kenai.jffi.CallContext;
import com.kenai.jffi.CallingConvention;
import com.kenai.jffi.HeapInvocationBuffer;
import com.kenai.jffi.Type;
import jnr.ffi.Platform;
import org.apache.directmemory.buffer.BufferUnderflowException;
import org.apache.directmemory.buffer.spi.Partition;
import org.apache.directmemory.buffer.spi.PartitionSlice;
import org.apache.directmemory.buffer.spi.PartitionSliceSelector;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class ProcessorLocalPartitionSliceSelector
    implements PartitionSliceSelector
{

    private static final NativeAdapter nativeAdapter = getNativeAdapterSPI();

    private final AtomicReferenceArray<Partition> cpuLocalPartition =
        new AtomicReferenceArray<Partition>( nativeAdapter.getProcessorCount() );

    private volatile int[] assigned = new int[0];

    @Override
    public PartitionSlice selectPartitionSlice( Partition[] partitions )
    {
        int processorId = nativeAdapter.getCurrentProcessorId();
        Partition partition = cpuLocalPartition.get( processorId );
        if ( partition != null && partition.available() > 0 )
        {
            PartitionSlice slice = partition.get();
            if ( slice != null )
            {
                return slice;
            }
        }

        synchronized ( assigned )
        {
            if ( assigned.length == 0 )
            {
                assigned = new int[partitions.length];
                Arrays.fill( assigned, -1 );
            }

            for ( int index = 0; index < assigned.length; index++ )
            {
                if ( assigned[index] == -1 )
                {
                    assigned[index] = processorId;
                    partition = partitions[index];
                    if ( partition.available() > 0 )
                    {
                        cpuLocalPartition.set( processorId, partition );
                        PartitionSlice slice = partition.get();
                        if ( slice != null )
                        {
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
        if ( partition.available() == partition.getSliceCount() )
        {
            assigned[partitionIndex] = -1;
        }
    }

    private static NativeAdapter getNativeAdapterSPI()
    {
        CallContext callContext = CallContext.getCallContext( Type.VOID, new Type[0], CallingConvention.DEFAULT, false );
        new HeapInvocationBuffer( callContext );

        String osName = System.getProperty( "os.name" );
        String osArch = System.getProperty( "os.arch" );
        String osVersion = System.getProperty( "os.version" );

        int processorCount = Runtime.getRuntime().availableProcessors();
        Platform platform = Platform.getNativePlatform();
        Platform.OS os = platform.getOS();
        if ( os == Platform.OS.LINUX )
        {
            return new LinuxNativeAdapter( processorCount );
        }
        else if ( os == Platform.OS.WINDOWS )
        {
            if ( osVersion != null && osVersion.startsWith( "6." ) )
            {
                return new WindowsNativeAdapter( processorCount );
            }
        }

        throw new UnsupportedOperationSystemException(
            "OS " + osName + " (" + osVersion + " / " + osArch + ") is unsupported for use of cpu local allocation strategy" );
    }

}
