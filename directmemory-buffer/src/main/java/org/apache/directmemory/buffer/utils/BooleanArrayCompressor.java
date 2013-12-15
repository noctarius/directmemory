package org.apache.directmemory.buffer.utils;

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

import org.apache.directmemory.buffer.ReadablePartitionBuffer;
import org.apache.directmemory.buffer.WritablePartitionBuffer;

public final class BooleanArrayCompressor
{

    private BooleanArrayCompressor()
    {
    }

    public static void writeBooleanArray( boolean[] booleans, WritablePartitionBuffer buffer )
    {
        int length = booleans.length;
        if ( length > 255 )
        {
            throw new IllegalArgumentException( "array must not be larger than 255 elements" );
        }
        int blocksize = length / 8 + ( length % 8 == 0 ? 0 : 1 );
        byte[] block = new byte[blocksize];
        for ( int i = 0; i < length; i++ )
        {
            int index = i / 8;
            int pos = i % 8;
            block[index] = (byte) ( block[index] | ( ( booleans[i] ? 1 : 0 ) << pos ) );
        }
        buffer.writeByte( length );
        buffer.writeBytes( block );
    }

    public static boolean[] readBooleanArray( ReadablePartitionBuffer buffer )
    {
        int length = buffer.readUnsignedByte();
        boolean[] booleans = new boolean[length];
        int blocksize = length / 8 + ( length % 8 == 0 ? 0 : 1 );
        byte[] block = new byte[blocksize];
        buffer.readBytes( block );
        for ( int i = 0; i < length; i++ )
        {
            int index = i / 8;
            int pos = i % 8;
            int mask = 1 << pos;
            booleans[i] = ( block[index] & mask ) == mask;
        }
        return booleans;
    }

}
