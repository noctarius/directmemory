package org.apache.directmemory.memory.buffer;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.directmemory.buffer.PartitionStrategy;
import org.apache.directmemory.buffer.impl.BufferUtils;
import org.apache.directmemory.buffer.spi.PartitionFactory;

final class PartitionBufferMemoryManagerHelper
{

    private static final String PB_PROPERTY_DISABLE_POOLING = "directmemory.buffer.pooling.disabled";

    private static final String PB_PROPERTY_ENABLE_UNSAFE = "directmemory.buffer.unsafe.enabled";

    private static final String PB_PROPERTY_ENABLE_OFFHEAP = "directmemory.buffer.offheap.enabled";

    private PartitionBufferMemoryManagerHelper()
    {
    }

    static PartitionFactory applyStrategies()
    {
        String property = System.getProperty( PB_PROPERTY_ENABLE_UNSAFE, "false" );
        boolean enableUnsafe = Boolean.parseBoolean( property ) && BufferUtils.isUnsafeAvailable();

        property = System.getProperty( PB_PROPERTY_ENABLE_OFFHEAP, "false" );
        boolean enableOffheap = Boolean.parseBoolean( property );

        property = System.getProperty( PB_PROPERTY_DISABLE_POOLING, "false" );
        boolean disablePooling = Boolean.parseBoolean( property );

        if ( !disablePooling )
        {
            if ( enableUnsafe )
            {
                return PartitionStrategy.POOLED_UNSAFE.getPartitionFactory();
            }
            if ( enableOffheap )
            {
                return PartitionStrategy.POOLED_BYTEBUFFER_DIRECT.getPartitionFactory();
            }
            return PartitionStrategy.POOLED_BYTEBUFFER_HEAP.getPartitionFactory();
        }

        if ( enableUnsafe )
        {
            return PartitionStrategy.UNPOOLED_UNSAFE.getPartitionFactory();
        }
        if ( enableOffheap )
        {
            return PartitionStrategy.UNPOOLED_BYTEBUFFER_DIRECT.getPartitionFactory();
        }
        return PartitionStrategy.UNPOOLED_BYTEBUFFER_HEAP.getPartitionFactory();
    }

}
