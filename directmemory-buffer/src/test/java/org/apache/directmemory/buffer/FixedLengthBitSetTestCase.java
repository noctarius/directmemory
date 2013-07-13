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

import static org.junit.Assert.assertEquals;

import org.apache.directmemory.buffer.impl.FixedLengthBitSet;
import org.junit.Test;

public class FixedLengthBitSetTestCase
{

    @Test
    public void testRanOutOfBits()
        throws Exception
    {
        FixedLengthBitSet bitSet = new FixedLengthBitSet( 100 );
        for ( int i = 0; i < 100; i++ )
        {
            int index = bitSet.nextNotSet( i );
            bitSet.testAndSet( index );
            assertEquals( index, i );
            assertEquals( bitSet.cardinality(), i + 1 );
        }

        assertEquals( -1, bitSet.nextNotSet( 0 ) );
    }

    @Test
    public void testNextNotSet1()
        throws Exception
    {
        FixedLengthBitSet bitSet = new FixedLengthBitSet( 10 );
        for ( int i = 0; i < 10; i++ )
        {
            int index = bitSet.nextNotSet( i );
            bitSet.testAndSet( index );
            assertEquals( index, i );
            assertEquals( bitSet.cardinality(), i + 1 );
        }

        bitSet.clear( 6 );
        assertEquals( 6, bitSet.nextNotSet( 9 ) );
        assertEquals( 6, bitSet.nextNotSet( 0 ) );
    }

    @Test
    public void testNextNotSet2()
        throws Exception
    {
        FixedLengthBitSet bitSet = new FixedLengthBitSet( 129 );
        for ( int i = 0; i < 129; i++ )
        {
            int index = bitSet.nextNotSet( i );
            bitSet.testAndSet( index );
            assertEquals( index, i );
            assertEquals( bitSet.cardinality(), i + 1 );
        }

        bitSet.clear( 6 );
        assertEquals( 6, bitSet.nextNotSet( 128 ) );
        assertEquals( 6, bitSet.nextNotSet( 0 ) );
    }

}
