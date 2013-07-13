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

public interface ReadablePartitionBuffer
{

    boolean readable();

    long readableSize();

    byte readByte();

    short readUnsignedByte();

    int readBytes( byte[] bytes );

    int readBytes( byte[] bytes, int offset, int length );

    boolean readBoolean();

    char readChar();

    double readDouble();

    double readCompressedDouble();

    float readFloat();

    float readCompressedFloat();

    long readLong();

    long readCompressedLong();

    short readShort();

    int readUnsignedShort();

    int readInt();

    String readString();

    int readCompressedInt();

    long readUnsignedInt();

    long readerIndex();

    void readerIndex( long readerIndex );

}
