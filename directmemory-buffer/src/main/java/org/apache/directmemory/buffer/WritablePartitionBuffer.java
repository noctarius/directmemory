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

import java.nio.ByteBuffer;

public interface WritablePartitionBuffer
{

    boolean writeable();

    void writeByte( int value );

    void writeBytes( byte[] bytes );

    void writeBytes( byte[] bytes, int offset, int length );

    void writeByteBuffer( ByteBuffer byteBuffer );

    void writeByteBuffer( ByteBuffer byteBuffer, int offset, int length );

    void writePartitionBuffer( ReadablePartitionBuffer partitionBuffer );

    void writePartitionBuffer( ReadablePartitionBuffer partitionBuffer, long offset, long length );

    void writeBoolean( boolean value );

    void writeChar( int value );

    void writeDouble( double value );

    void writeCompressedDouble( double value );

    void writeFloat( float value );

    void writeCompressedFloat( float value );

    void writeLong( long value );

    void writeCompressedLong( long value );

    void writeShort( short value );

    void writeInt( int value );

    void writeCompressedInt( int value );

    void writeString( String value );

    long writerIndex();

    void writerIndex( long writerIndex );

    void flush();

}
