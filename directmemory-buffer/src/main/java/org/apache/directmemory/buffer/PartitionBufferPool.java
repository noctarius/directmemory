package org.apache.directmemory.buffer;

import java.io.Closeable;

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

public interface PartitionBufferPool
    extends Closeable
{

    PartitionBuffer getPartitionBuffer();

    PartitionBuffer getPartitionBuffer( long size );

    void freePartitionBuffer( PartitionBuffer partitionBuffer );

    long getTotalByteSize();

    int getPartitionByteSize();

    int getPartitionCount();

    int getSliceCountPerPartition();

    int getSliceCount();

    int getSliceByteSize();

    int getUsedSliceCount();

    int getFreeSliceCount();

    void close();

    boolean isPooled();

}
