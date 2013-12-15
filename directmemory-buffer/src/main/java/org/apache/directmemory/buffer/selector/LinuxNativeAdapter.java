/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.directmemory.buffer.selector;

import jnr.ffi.LibraryLoader;
import jnr.ffi.Platform;

class LinuxNativeAdapter
    implements NativeAdapter
{

    private static final Platform PLATFORM = Platform.getNativePlatform();

    private static final String LIB_C = PLATFORM.mapLibraryName( "c" );

    private static final LinuxNative LINUX_NATIVE = LibraryLoader.<LinuxNative>create( LinuxNative.class ).load( LIB_C );

    private final int processorCount;

    LinuxNativeAdapter( int processorCount )
    {
        this.processorCount = processorCount;
    }

    @Override
    public int getProcessorCount()
    {
        return processorCount;
    }

    @Override
    public int getCurrentProcessorId()
    {
        return LINUX_NATIVE.sched_getcpu();
    }

}
