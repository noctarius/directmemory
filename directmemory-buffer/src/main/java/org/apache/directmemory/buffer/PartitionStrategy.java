package org.apache.directmemory.buffer;

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

import org.apache.directmemory.buffer.impl.ByteBufferPooledPartition;
import org.apache.directmemory.buffer.impl.ByteBufferUnpooledPartition;
import org.apache.directmemory.buffer.impl.UnsafePooledPartition;
import org.apache.directmemory.buffer.impl.UnsafeUnpooledPartition;
import org.apache.directmemory.buffer.spi.PartitionFactory;

public enum PartitionStrategy
{

    POOLED_UNSAFE( UnsafePooledPartition.UNSAFE_PARTITION_FACTORY ), //
    POOLED_BYTEBUFFER_HEAP( ByteBufferPooledPartition.HEAP_BYTEBUFFER_PARTITION_FACTORY ), //
    POOLED_BYTEBUFFER_DIRECT( ByteBufferPooledPartition.DIRECT_BYTEBUFFER_PARTITION_FACTORY ), //
    UNPOOLED_UNSAFE( UnsafeUnpooledPartition.UNSAFE_PARTITION_FACTORY ), //
    UNPOOLED_BYTEBUFFER_HEAP( ByteBufferUnpooledPartition.HEAP_BYTEBUFFER_PARTITION_FACTORY ), //
    UNPOOLED_BYTEBUFFER_DIRECT( ByteBufferUnpooledPartition.DIRECT_BYTEBUFFER_PARTITION_FACTORY ), //
    ;

    private final PartitionFactory partitionFactory;

    private PartitionStrategy( PartitionFactory partitionFactory )
    {
        this.partitionFactory = partitionFactory;
    }

    public PartitionFactory getPartitionFactory()
    {
        return partitionFactory;
    }

}
