package org.apache.directmemory.memory;

import java.util.Arrays;
import java.util.Collection;

import org.apache.directmemory.memory.allocator.AllocatorMemoryManagerFactory;
import org.apache.directmemory.memory.unsafe.UnsafeMemoryManagerFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

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

@RunWith( Parameterized.class )
public class DefaultMemoryManagerServiceTest
    extends AbstractMemoryManagerServiceTest
{

    @Parameters
    public static Collection<Object[]> data()
    {
        return Arrays.asList( new Object[][] { { AllocatorMemoryManagerFactory.class },
            { UnsafeMemoryManagerFactory.class } } );
    }

    private final Class<? extends MemoryManagerFactory<Object>> memoryManagerFactoryClass;

    public DefaultMemoryManagerServiceTest( Class<? extends MemoryManagerFactory<Object>> memoryManagerFactoryClass )
    {
        this.memoryManagerFactoryClass = memoryManagerFactoryClass;
    }

    @Override
    protected MemoryManager<Object> instanciateMemoryManagerService( int bufferSize )
    {
        try
        {
            final MemoryManagerFactory<Object> factory = memoryManagerFactoryClass.newInstance();
            mms = factory.build( 1, 1, bufferSize );
            return mms;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

}
