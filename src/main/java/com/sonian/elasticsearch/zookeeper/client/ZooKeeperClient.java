/*
 * Copyright 2011 Sonian Inc.
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


package com.sonian.elasticsearch.zookeeper.client;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Set;

/**
 * @author imotov
 */
public interface ZooKeeperClient extends LifecycleComponent<ZooKeeperClient> {

    void createPersistentNode(String path) throws InterruptedException;

    void setOrCreatePersistentNode(String path, byte[] data) throws InterruptedException;

    byte[] getOrCreateTransientNode(String path, byte[] data, NodeListener nodeListener) throws InterruptedException;

    void setOrCreateTransientNode(String path, byte[] data) throws InterruptedException;


    byte[] getNode(final String path, final NodeListener nodeListener) throws InterruptedException;

    Set<String> listNodes(String path, NodeListChangedListener nodeListChangedListener) throws InterruptedException;

    void deleteNode(String path) throws InterruptedException;

    void deleteNodeRecursively(String path) throws InterruptedException;


    String createLargeSequentialNode(String pathPrefix, byte[] data) throws InterruptedException;

    void deleteLargeNode(String path) throws InterruptedException;

    byte[] getLargeNode(final String path) throws InterruptedException;


    void addSessionStateListener(SessionStateListener sessionStateListener);

    void removeSessionStateListener(SessionStateListener sessionStateListener);

    boolean verifyConnection(TimeValue timeout) throws InterruptedException ;

    boolean connected();

    long sessionId();

    interface SessionStateListener {
        public void sessionDisconnected();
        public void sessionConnected();
        public void sessionExpired();
    }

    interface NodeListener {
        public void onNodeCreated(String id);

        public void onNodeDeleted(String id);

        public void onNodeDataChanged(String id);
    }

    interface NodeListChangedListener {
        public void onNodeListChanged();
    }
}
