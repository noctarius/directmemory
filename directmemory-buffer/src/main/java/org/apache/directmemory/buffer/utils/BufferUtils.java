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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import org.apache.directmemory.buffer.PartitionBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings( "restriction" )
public final class BufferUtils
{

    private static final Logger LOGGER = LoggerFactory.getLogger( BufferUtils.class );

    public static final int BYTE_ARRAY_OFFSET;

    private static final long KILOBYTE_BYTE_SIZE = 1024;

    private static final long MEGABYTE_BYTE_SIZE = KILOBYTE_BYTE_SIZE * 1024;

    private static final long GIGABYTE_BYTE_SIZE = MEGABYTE_BYTE_SIZE * 1024;

    private static final long TERABYTE_BYTE_SIZE = GIGABYTE_BYTE_SIZE * 1024;

    private static final Method DIRECT_BYTE_BUFFER_CLEAN;

    private static final Method DIRECT_BYTE_BUFFER_CLEANER;

    private static final boolean CLEANER_AVAILABLE;

    private static final sun.misc.Unsafe UNSAFE;

    private static final boolean UNSAFE_AVAILABLE;

    static
    {
        Method directByteBufferClean = null;
        Method directByteBufferCleaner = null;
        try
        {
            Class<?> clazz = Class.forName( "java.nio.DirectByteBuffer" );
            directByteBufferCleaner = clazz.getDeclaredMethod( "cleaner" );
            directByteBufferCleaner.setAccessible( true );

            clazz = Class.forName( "sun.misc.Cleaner" );
            directByteBufferClean = clazz.getDeclaredMethod( "clean" );
            directByteBufferClean.setAccessible( true );
        }
        catch ( Exception e )
        {
            // Ignore since DirectByteBuffer or clean() method aren't available on this JVM
        }
        DIRECT_BYTE_BUFFER_CLEAN = directByteBufferClean;
        DIRECT_BYTE_BUFFER_CLEANER = directByteBufferCleaner;
        CLEANER_AVAILABLE = DIRECT_BYTE_BUFFER_CLEAN != null && DIRECT_BYTE_BUFFER_CLEANER != null;

        sun.misc.Unsafe unsafe = null;
        try
        {
            Field unsafeField = sun.misc.Unsafe.class.getDeclaredField( "theUnsafe" );
            unsafeField.setAccessible( true );
            unsafe = (sun.misc.Unsafe) unsafeField.get( null );
        }
        catch ( Exception e )
        {
            // Ignore thins sun.misc.Unsafe doesn't seem to exists on this JVM
        }

        UNSAFE = unsafe;
        UNSAFE_AVAILABLE = UNSAFE != null;
        BYTE_ARRAY_OFFSET = UNSAFE_AVAILABLE ? UNSAFE.arrayBaseOffset( byte[].class ) : -1;
    }

    private BufferUtils()
    {
    }

    public static boolean isUnsafeAvailable()
    {
        return UNSAFE_AVAILABLE;
    }

    public static sun.misc.Unsafe getUnsafe()
    {
        return UNSAFE;
    }

    public static void cleanByteBuffer( ByteBuffer byteBuffer )
    {
        if ( !byteBuffer.isDirect() || !CLEANER_AVAILABLE )
        {
            return;
        }

        try
        {
            Object cleaner = DIRECT_BYTE_BUFFER_CLEANER.invoke( byteBuffer );
            DIRECT_BYTE_BUFFER_CLEAN.invoke( cleaner );
        }
        catch ( Exception e )
        {
            LOGGER.debug( "DirectByteBuffer could not be cleaned", e );
        }
    }

    public static void putShort( short value, PartitionBuffer buffer, boolean bigEndian )
    {
        if ( bigEndian )
        {
            buffer.writeByte( (byte) ( value >> 8 ) );
            buffer.writeByte( (byte) ( value >> 0 ) );
        }
        else
        {
            buffer.writeByte( (byte) ( value >> 0 ) );
            buffer.writeByte( (byte) ( value >> 8 ) );
        }
    }

    public static short getShort( PartitionBuffer buffer, boolean bigEndian )
    {
        if ( bigEndian )
        {
            byte b1 = buffer.readByte();
            byte b0 = buffer.readByte();
            return buildShort( b1, b0 );
        }
        else
        {
            byte b0 = buffer.readByte();
            byte b1 = buffer.readByte();
            return buildShort( b1, b0 );
        }
    }

    public static void putInt( int value, PartitionBuffer buffer, boolean bigEndian )
    {
        int index = 0;
        byte[] data = new byte[4];
        if ( bigEndian )
        {
            data[index++] = (byte) ( value >>> 24 );
            data[index++] = (byte) ( value >>> 16 );
            data[index++] = (byte) ( value >>> 8 );
            data[index++] = (byte) ( value >>> 0 );
        }
        else
        {
            data[index++] = (byte) ( value >>> 0 );
            data[index++] = (byte) ( value >>> 8 );
            data[index++] = (byte) ( value >>> 16 );
            data[index++] = (byte) ( value >>> 24 );
        }
        buffer.writeBytes( data );
    }

    public static int getInt( PartitionBuffer buffer, boolean bigEndian )
    {
        byte[] data = new byte[4];
        buffer.readBytes( data );
        if ( bigEndian )
        {
            byte b3 = data[0];
            byte b2 = data[1];
            byte b1 = data[2];
            byte b0 = data[3];
            return buildInt( b3, b2, b1, b0 );
        }
        else
        {
            byte b0 = data[0];
            byte b1 = data[1];
            byte b2 = data[2];
            byte b3 = data[3];
            return buildInt( b3, b2, b1, b0 );
        }
    }

    public static void putLong( long value, PartitionBuffer buffer, boolean bigEndian )
    {
        int index = 0;
        byte[] data = new byte[8];
        if ( bigEndian )
        {
            data[index++] = (byte) ( value >> 56 );
            data[index++] = (byte) ( value >> 48 );
            data[index++] = (byte) ( value >> 40 );
            data[index++] = (byte) ( value >> 32 );
            data[index++] = (byte) ( value >> 24 );
            data[index++] = (byte) ( value >> 16 );
            data[index++] = (byte) ( value >> 8 );
            data[index++] = (byte) ( value >> 0 );
        }
        else
        {
            data[index++] = (byte) ( value >> 0 );
            data[index++] = (byte) ( value >> 8 );
            data[index++] = (byte) ( value >> 16 );
            data[index++] = (byte) ( value >> 24 );
            data[index++] = (byte) ( value >> 32 );
            data[index++] = (byte) ( value >> 40 );
            data[index++] = (byte) ( value >> 48 );
            data[index++] = (byte) ( value >> 56 );
        }
        buffer.writeBytes( data );
    }

    public static long getLong( PartitionBuffer buffer, boolean bigEndian )
    {
        byte[] data = new byte[8];
        buffer.readBytes( data );
        if ( bigEndian )
        {
            byte b7 = data[0];
            byte b6 = data[1];
            byte b5 = data[2];
            byte b4 = data[3];
            byte b3 = data[4];
            byte b2 = data[5];
            byte b1 = data[6];
            byte b0 = data[7];
            return buildLong( b7, b6, b5, b4, b3, b2, b1, b0 );
        }
        else
        {
            byte b0 = data[0];
            byte b1 = data[1];
            byte b2 = data[2];
            byte b3 = data[3];
            byte b4 = data[4];
            byte b5 = data[5];
            byte b6 = data[6];
            byte b7 = data[7];
            return buildLong( b7, b6, b5, b4, b3, b2, b1, b0 );
        }
    }

    public static void rangeCheck( int position, int lowerBound, int upperBound, String name )
    {
        if ( position < lowerBound )
        {
            throw new IndexOutOfBoundsException( "Given value " + name + " is smaller than lower bound " + lowerBound );
        }
        if ( position >= upperBound )
        {
            throw new IndexOutOfBoundsException( "Given value " + name + " is larger than upper bound " + upperBound );
        }
    }

    public static boolean isPowerOfTwo( long value )
    {
        return value > 0 && (value & -value) == value;
    }

    public static long descriptorToByteSize( String descriptor )
    {
        // Trim possible whitespaces
        descriptor = descriptor.trim();

        char descriptorChar = descriptor.charAt( descriptor.length() - 1 );
        if ( Character.isDigit( descriptorChar ) )
        {
            throw new IllegalArgumentException( "Descriptor char " + descriptorChar
                + " is no legal size descriptor (only B, K, M, G, T allowed)" );
        }

        for ( int i = 0; i < descriptor.length() - 2; i++ )
        {
            if ( !Character.isDigit( descriptor.charAt( i ) ) )
            {
                throw new IllegalArgumentException( "Non digit character at position " + i );
            }
        }

        double value = Double.parseDouble( descriptor.substring( 0, descriptor.length() - 1 ) );
        switch ( descriptorChar )
        {
            case 'b':
            case 'B':
                return Double.valueOf( value ).longValue();

            case 'k':
            case 'K':
                return Double.valueOf( value * KILOBYTE_BYTE_SIZE ).longValue();

            case 'm':
            case 'M':
                return Double.valueOf( value * MEGABYTE_BYTE_SIZE ).longValue();

            case 'g':
            case 'G':
                return Double.valueOf( value * GIGABYTE_BYTE_SIZE ).longValue();

            case 't':
            case 'T':
                return Double.valueOf( value * TERABYTE_BYTE_SIZE ).longValue();
        }

        throw new IllegalArgumentException( "Descriptor character " + descriptorChar
            + " is unknown (only B, K, M, G, T allowed)" );
    }

    private static short buildShort( byte b1, byte b0 )
    {
        return (short) ( ( ( ( b1 & 0xFF ) << 8 ) | ( ( b0 & 0xFF ) << 0 ) ) );
    }

    private static int buildInt( byte b3, byte b2, byte b1, byte b0 )
    {
        return ( ( ( ( b3 & 0xFF ) << 24 ) | ( ( b2 & 0xFF ) << 16 ) | ( ( b1 & 0xFF ) << 8 ) | ( ( b0 & 0xFF ) << 0 ) ) );
    }

    private static long buildLong( byte b7, byte b6, byte b5, byte b4, byte b3, byte b2, byte b1, byte b0 )
    {
        return ( ( ( ( b7 & 0xFFL ) << 56 ) | ( ( b6 & 0xFFL ) << 48 ) | ( ( b5 & 0xFFL ) << 40 )
            | ( ( b4 & 0xFFL ) << 32 ) | ( ( b3 & 0xFFL ) << 24 ) | ( ( b2 & 0xFFL ) << 16 ) | ( ( b1 & 0xFFL ) << 8 ) | ( ( b0 & 0xFFL ) << 0 ) ) );
    }

}
