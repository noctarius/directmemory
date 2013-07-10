package org.apache.directmemory.buffer.impl;

import org.apache.directmemory.buffer.ReadablePartitionBuffer;
import org.apache.directmemory.buffer.WritablePartitionBuffer;

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

public final class Int64Compressor
{

    private Int64Compressor()
    {
    }

    static final byte INT64_FULL = 1;

    static final byte INT64_COMPRESSED_SINGLE = 2;

    static final byte INT64_COMPRESSED_DOUBLE = 3;

    static final byte INT64_COMPRESSED_TRIPPLE = 4;

    static final byte INT64_COMPRESSED_QUAD = 5;

    static final byte INT64_COMPRESSED_FIFTH = 6;

    static final byte INT64_COMPRESSED_SIXTH = 7;

    static final byte INT64_COMPRESSED_SEVENTH = 8;

    static final long INT64_MAX_SINGLE = 0x7F;

    static final long INT64_MIN_SINGLE = ~INT64_MAX_SINGLE + 1;

    static final long INT64_MAX_DOUBLE = 0x7FFF;

    static final long INT64_MIN_DOUBLE = ~INT64_MAX_DOUBLE + 1;

    static final long INT64_MAX_TRIPPLE = 0x7FFFFF;

    static final long INT64_MIN_TRIPPLE = ~INT64_MAX_TRIPPLE + 1;

    static final long INT64_MAX_QUAD = 0x7FFFFFFF;

    static final long INT64_MIN_QUAD = ~INT64_MAX_QUAD + 1;

    static final long INT64_MAX_FIFTH = 0x7FFFFFFFFFL;

    static final long INT64_MIN_FIFTH = ~INT64_MAX_FIFTH + 1;

    static final long INT64_MAX_SIXTH = 0x7FFFFFFFFFFFL;

    static final long INT64_MIN_SIXTH = ~INT64_MAX_SIXTH + 1;

    static final long INT64_MAX_SEVENTH = 0x7FFFFFFFFFFFFFL;

    static final long INT64_MIN_SEVENTH = ~INT64_MAX_SEVENTH + 1;

    public static void writeInt64( long value, WritablePartitionBuffer buffer )
    {
        if ( value >= INT64_MIN_SINGLE && value <= INT64_MAX_SINGLE )
        {
            value = value < 0 ? ( ~value + 1 ) | ( 1L << 7 ) : value;
            buffer.writeByte( INT64_COMPRESSED_SINGLE );
            buffer.writeByte( (byte) value );
        }
        else if ( value >= INT64_MIN_DOUBLE && value <= INT64_MAX_DOUBLE )
        {
            value = value < 0 ? ( ~value + 1 ) | ( 1L << 15 ) : value;
            buffer.writeByte( INT64_COMPRESSED_DOUBLE );
            buffer.writeByte( (byte) ( value >> 8 ) );
            buffer.writeByte( (byte) value );
        }
        else if ( value >= INT64_MIN_TRIPPLE && value <= INT64_MAX_TRIPPLE )
        {
            value = value < 0 ? ( ~value + 1 ) | ( 1L << 23 ) : value;
            buffer.writeByte( INT64_COMPRESSED_TRIPPLE );
            buffer.writeByte( (byte) ( value >> 16 ) );
            buffer.writeByte( (byte) ( value >> 8 ) );
            buffer.writeByte( (byte) value );
        }
        else if ( value >= INT64_MIN_QUAD && value <= INT64_MAX_QUAD )
        {
            value = value < 0 ? ( ~value + 1 ) | ( 1L << 31 ) : value;
            buffer.writeByte( INT64_COMPRESSED_QUAD );
            buffer.writeByte( (byte) ( value >> 24 ) );
            buffer.writeByte( (byte) ( value >> 16 ) );
            buffer.writeByte( (byte) ( value >> 8 ) );
            buffer.writeByte( (byte) value );
        }
        else if ( value >= INT64_MIN_FIFTH && value <= INT64_MAX_FIFTH )
        {
            value = value < 0 ? ( ~value + 1 ) | ( 1L << 39 ) : value;
            buffer.writeByte( INT64_COMPRESSED_FIFTH );
            buffer.writeByte( (byte) ( value >> 32 ) );
            buffer.writeByte( (byte) ( value >> 24 ) );
            buffer.writeByte( (byte) ( value >> 16 ) );
            buffer.writeByte( (byte) ( value >> 8 ) );
            buffer.writeByte( (byte) value );
        }
        else if ( value >= INT64_MIN_SIXTH && value <= INT64_MAX_SIXTH )
        {
            value = value < 0 ? ( ~value + 1 ) | ( 1L << 47 ) : value;
            buffer.writeByte( INT64_COMPRESSED_SIXTH );
            buffer.writeByte( (byte) ( value >> 40 ) );
            buffer.writeByte( (byte) ( value >> 32 ) );
            buffer.writeByte( (byte) ( value >> 24 ) );
            buffer.writeByte( (byte) ( value >> 16 ) );
            buffer.writeByte( (byte) ( value >> 8 ) );
            buffer.writeByte( (byte) value );
        }
        else if ( value >= INT64_MIN_SEVENTH && value <= INT64_MAX_SEVENTH )
        {
            value = value < 0 ? ( ~value + 1 ) | ( 1L << 55 ) : value;
            buffer.writeByte( INT64_COMPRESSED_SEVENTH );
            buffer.writeByte( (byte) ( value >> 48 ) );
            buffer.writeByte( (byte) ( value >> 40 ) );
            buffer.writeByte( (byte) ( value >> 32 ) );
            buffer.writeByte( (byte) ( value >> 24 ) );
            buffer.writeByte( (byte) ( value >> 16 ) );
            buffer.writeByte( (byte) ( value >> 8 ) );
            buffer.writeByte( (byte) value );
        }
        else
        {
            buffer.writeByte( INT64_FULL );
            buffer.writeByte( (byte) ( value >> 56 ) );
            buffer.writeByte( (byte) ( value >> 48 ) );
            buffer.writeByte( (byte) ( value >> 40 ) );
            buffer.writeByte( (byte) ( value >> 32 ) );
            buffer.writeByte( (byte) ( value >> 24 ) );
            buffer.writeByte( (byte) ( value >> 16 ) );
            buffer.writeByte( (byte) ( value >> 8 ) );
            buffer.writeByte( (byte) value );
        }
    }

    public static long readInt64( ReadablePartitionBuffer buffer )
    {
        byte type = buffer.readByte();
        switch ( type )
        {
            case INT64_COMPRESSED_SINGLE:
            {
                long data = buffer.readByte() & 0xFFL;
                return ( ( data >> 7 ) & 1 ) == 1 ? ~( data ^ ( 1L << 7 ) ) + 1 : data;
            }

            case INT64_COMPRESSED_DOUBLE:
            {
                long data = ( ( buffer.readByte() & 0xFFL ) << 8 ) | ( buffer.readByte() & 0xFFL );
                return ( ( data >> 15 ) & 1 ) == 1 ? ~( data ^ ( 1L << 15 ) ) + 1 : data;
            }

            case INT64_COMPRESSED_TRIPPLE:
            {
                long data =
                    ( ( buffer.readByte() & 0xFFL ) << 16 ) | ( ( buffer.readByte() & 0xFFL ) << 8 )
                        | ( buffer.readByte() & 0xFFL );
                return ( ( data >> 23 ) & 1 ) == 1 ? ~( data ^ ( 1L << 23 ) ) + 1 : data;
            }
            case INT64_COMPRESSED_QUAD:
            {
                byte[] bucket = new byte[4];
                buffer.readBytes( bucket );
                long data =
                    ( ( bucket[0] & 0xFFL ) << 24 ) | ( ( bucket[1] & 0xFFL ) << 16 ) | ( ( bucket[2] & 0xFFL ) << 8 )
                        | ( bucket[3] & 0xFFL );
                return ( ( data >> 31 ) & 1 ) == 1 ? ~( data ^ ( 1L << 31 ) ) + 1 : data;
            }
            case INT64_COMPRESSED_FIFTH:
            {
                byte[] bucket = new byte[5];
                buffer.readBytes( bucket );
                long data =
                    ( ( bucket[0] & 0xFFL ) << 32 ) | ( ( bucket[1] & 0xFFL ) << 24 ) | ( ( bucket[2] & 0xFFL ) << 16 )
                        | ( ( bucket[3] & 0xFFL ) << 8 ) | ( bucket[4] & 0xFFL );
                return ( ( data >> 39 ) & 1 ) == 1 ? ~( data ^ ( 1L << 39 ) ) + 1 : data;
            }
            case INT64_COMPRESSED_SIXTH:
            {
                byte[] bucket = new byte[6];
                buffer.readBytes( bucket );
                long data =
                    ( ( bucket[0] & 0xFFL ) << 40 ) | ( ( bucket[1] & 0xFFL ) << 32 ) | ( ( bucket[2] & 0xFFL ) << 24 )
                        | ( ( bucket[3] & 0xFFL ) << 16 ) | ( ( bucket[4] & 0xFFL ) << 8 ) | ( bucket[5] & 0xFFL );
                return ( ( data >> 47 ) & 1 ) == 1 ? ~( data ^ ( 1L << 47 ) ) + 1 : data;
            }
            case INT64_COMPRESSED_SEVENTH:
            {
                byte[] bucket = new byte[7];
                buffer.readBytes( bucket );
                long data =
                    ( ( bucket[0] & 0xFFL ) << 48 ) | ( ( bucket[1] & 0xFFL ) << 40 ) | ( ( bucket[2] & 0xFFL ) << 32 )
                        | ( ( bucket[3] & 0xFFL ) << 24 ) | ( ( bucket[4] & 0xFFL ) << 16 )
                        | ( ( bucket[5] & 0xFFL ) << 8 ) | ( bucket[6] & 0xFFL );
                return ( ( data >> 55 ) & 1 ) == 1 ? ~( data ^ ( 1L << 55 ) ) + 1 : data;
            }
        }

        byte[] bucket = new byte[8];
        buffer.readBytes( bucket );
        return ( ( bucket[0] & 0xFFL ) << 56 ) | ( ( bucket[1] & 0xFFL ) << 48 ) | ( ( bucket[2] & 0xFFL ) << 40 )
            | ( ( bucket[3] & 0xFFL ) << 32 ) | ( ( bucket[4] & 0xFFL ) << 24 ) | ( ( bucket[5] & 0xFFL ) << 16 )
            | ( ( bucket[6] & 0xFFL ) << 8 ) | ( bucket[7] & 0xFFL );
    }

}
