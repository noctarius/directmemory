package org.apache.directmemory.server.client;
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

import org.apache.directmemory.server.commons.DirectMemoryException;
import org.apache.directmemory.server.commons.DirectMemoryRequest;
import org.apache.directmemory.server.commons.DirectMemoryResponse;

import java.io.IOException;
import java.util.concurrent.Future;

/**
 * @author Olivier Lamy
 */
public interface DirectMemoryClient
{

    DirectMemoryResponse retrieve( DirectMemoryRequest directMemoryRequest )
        throws DirectMemoryException, IOException, ClassNotFoundException, InstantiationException,
        IllegalAccessException;

    Future<DirectMemoryResponse> asyncRetrieve( DirectMemoryRequest directMemoryRequest )
        throws DirectMemoryException;

    void put( DirectMemoryRequest directMemoryRequest )
        throws DirectMemoryException;

    Future<Void> asyncPut( DirectMemoryRequest directMemoryRequest )
        throws DirectMemoryException;

    DirectMemoryResponse delete( DirectMemoryRequest directMemoryRequest )
        throws DirectMemoryException;

    Future<DirectMemoryResponse> asyncDelete( DirectMemoryRequest directMemoryRequest )
        throws DirectMemoryException;
}