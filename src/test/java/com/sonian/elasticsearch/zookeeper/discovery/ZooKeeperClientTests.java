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

package com.sonian.elasticsearch.zookeeper.discovery;

import com.sonian.elasticsearch.zookeeper.client.ZooKeeperClientException;
import org.elasticsearch.ElasticSearchException;
import com.sonian.elasticsearch.zookeeper.client.AbstractNodeListener;
import com.sonian.elasticsearch.zookeeper.client.ZooKeeperClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author imotov
 */
public class ZooKeeperClientTests extends AbstractZooKeeperTests {

    @BeforeClass
    public void createTestPaths() throws Exception {
        startZooKeeper();
        buildZooKeeper().createPersistentNode("/tests/nodes");
    }

    @AfterClass
    public void shutdownZooKeeper() {
        stopZooKeeper();
    }

    @Test
    public void testElectionSequence() throws Exception {
        ZooKeeperClient zk1 = buildZooKeeper();
        ZooKeeperClient zk2 = buildZooKeeper();
        final boolean[] callbackForSelf = new boolean[1];

        assertThat(zk1.getOrCreateTransientNode("/tests/master", "id1".getBytes(), new AbstractNodeListener() {
            @Override
            public void onNodeDeleted(String id) {
                callbackForSelf[0] = true;
            }
        }), equalTo("id1".getBytes()));

        final CountDownLatch latch = new CountDownLatch(1);

        assertThat(zk2.getOrCreateTransientNode("/tests/master", "id2".getBytes(), new AbstractNodeListener() {
            @Override
            public void onNodeDeleted(String id) {
                latch.countDown();
            }
        }), equalTo("id1".getBytes()));

        zk1.stop();

        assertThat(latch.await(10, TimeUnit.SECONDS), equalTo(true));

        assertThat(zk2.getOrCreateTransientNode("/tests/master", "id2".getBytes(), null), equalTo("id2".getBytes()));
        assertThat(callbackForSelf[0], equalTo(false));

    }

    @Test
    public void testThreeNodeElection() throws Exception {
        ZooKeeperClient zk1 = buildZooKeeper();
        final ZooKeeperClient zk2 = buildZooKeeper();
        final ZooKeeperClient zk3 = buildZooKeeper();
        final byte[][] masters = new byte[2][];

        assertThat(zk1.getOrCreateTransientNode("/tests/master", "id1".getBytes(), null), equalTo("id1".getBytes()));

        final CountDownLatch latch = new CountDownLatch(2);

        assertThat(zk2.getOrCreateTransientNode("/tests/master", "id2".getBytes(), new AbstractNodeListener() {
            @Override
            public void onNodeDeleted(String id) {
                try {
                    masters[0] = zk2.getOrCreateTransientNode("/tests/master", "id2".getBytes(), null);
                } catch (InterruptedException ex) {
                    throw new ElasticSearchException("Thread interrupted", ex);
                }
                latch.countDown();
            }
        }), equalTo("id1".getBytes()));

        assertThat(zk3.getOrCreateTransientNode("/tests/master", "id3".getBytes(), new AbstractNodeListener() {
            @Override
            public void onNodeDeleted(String id) {
                try {
                    masters[1] = zk2.getOrCreateTransientNode("/tests/master", "id2".getBytes(), null);
                } catch (InterruptedException ex) {
                    throw new ElasticSearchException("Thread interrupted", ex);
                }
                latch.countDown();
            }
        }), equalTo("id1".getBytes()));

        zk1.stop();

        assertThat(latch.await(5, TimeUnit.SECONDS), equalTo(true));

        assertThat(masters[0], anyOf(equalTo("id2".getBytes()), equalTo("id3".getBytes())));
        assertThat(masters[0], equalTo(masters[1]));
        logger.error("New Master is " + masters[0]);
    }

    @Test
    public void testRegisterNode() throws Exception {
        ZooKeeperClient zk1 = buildZooKeeper();

        zk1.setOrCreateTransientNode("/tests/nodes/node1", "node1data".getBytes());
        assertThat(zk1.listNodes("/tests/nodes", null).contains("node1"), equalTo(true));
        zk1.deleteNode("/tests/nodes/node1");
        assertThat(zk1.listNodes("/tests/nodes", null).contains("node1"), equalTo(false));

    }

    private class RelistListener implements ZooKeeperClient.NodeListChangedListener {

        private ZooKeeperClient zk;
        private List<List<String>> lists;
        private volatile CountDownLatch latch;

        public RelistListener(ZooKeeperClient zk, List<List<String>> lists) {
            this.zk = zk;
            this.lists = lists;
            this.latch = new CountDownLatch(1);
        }

        @Override
        public synchronized void onNodeListChanged() {
            try {
                Set<String> res = zk.listNodes("/tests/nodes", this);
                List<String> resList = new ArrayList<String>(res);
                Collections.sort(resList);
                lists.add(resList);
                latch.countDown();
            } catch (InterruptedException ex) {
                throw new ElasticSearchException("Thread interrupted", ex);
            }
        }

        public boolean await() throws InterruptedException {
            return latch.await(5, TimeUnit.SECONDS);
        }

        public synchronized void resetLatch() {
            latch = new CountDownLatch(1);
        }

    }

    @Test
    public void testListNodes() throws Exception {
        List<List<String>> lists = new ArrayList<List<String>>();
        ZooKeeperClient zk1 = buildZooKeeper();
        RelistListener listener = new RelistListener(zk1, lists);
        assertThat(zk1.listNodes("/tests/nodes", listener).size(), equalTo(0));
        zk1.setOrCreateTransientNode("/tests/nodes/id1", "id1".getBytes());
        assertThat(listener.await(), equalTo(true));
        listener.resetLatch();
        zk1.setOrCreateTransientNode("/tests/nodes/id2", "id2".getBytes());
        assertThat(listener.await(), equalTo(true));
        listener.resetLatch();
        zk1.setOrCreateTransientNode("/tests/nodes/id3", "id3".getBytes());
        assertThat(listener.await(), equalTo(true));
        listener.resetLatch();
        zk1.deleteNode("/tests/nodes/id2");
        assertThat(listener.await(), equalTo(true));
        listener.resetLatch();

        assertThat(lists.get(0).toArray(), equalTo(new Object[]{"id1"}));
        assertThat(lists.get(1).toArray(), equalTo(new Object[]{"id1", "id2"}));
        assertThat(lists.get(2).toArray(), equalTo(new Object[]{"id1", "id2", "id3"}));
        assertThat(lists.get(3).toArray(), equalTo(new Object[]{"id1", "id3"}));
        assertThat(lists.size(), equalTo(4));
        zk1.listNodes("/tests/nodes", null);
    }

    @Test
    public void testFindMasterWithNoInitialMaster() throws Exception {
        ZooKeeperClient zk1 = buildZooKeeper();
        ZooKeeperClient zk2 = buildZooKeeper();
        final AtomicBoolean deletedCalled = new AtomicBoolean();
        final CountDownLatch latch = new CountDownLatch(1);

        assertThat(zk1.getNode("/tests/master", new AbstractNodeListener() {
            @Override
            public void onNodeCreated(String id) {
                latch.countDown();
            }

            @Override
            public void onNodeDeleted(String id) {
                deletedCalled.set(true);
            }
        }), nullValue());

        assertThat(zk2.getOrCreateTransientNode("/tests/master", "node1".getBytes(), null), equalTo("node1".getBytes()));
        assertThat(latch.await(5, TimeUnit.SECONDS), equalTo(true));
        assertThat(deletedCalled.get(), equalTo(false));

    }

    @Test
    public void testFindMasterWithInitialMaster() throws Exception {
        ZooKeeperClient zk1 = buildZooKeeper();
        ZooKeeperClient zk2 = buildZooKeeper();
        final AtomicBoolean createdCalled = new AtomicBoolean();
        final AtomicBoolean deletedCalled = new AtomicBoolean();
        final CountDownLatch latch = new CountDownLatch(1);
        assertThat(zk1.getOrCreateTransientNode("/tests/master", "node1".getBytes(), null), equalTo("node1".getBytes()));
        assertThat(zk2.getNode("/tests/master", new AbstractNodeListener() {
            @Override
            public void onNodeCreated(String id) {
                createdCalled.set(true);
            }

            @Override
            public void onNodeDeleted(String id) {
                latch.countDown();
            }
        }), equalTo("node1".getBytes()));

        assertThat(deletedCalled.get(), equalTo(false));
        zk1.stop();
        assertThat(latch.await(5, TimeUnit.SECONDS), equalTo(true));
        assertThat(createdCalled.get(), equalTo(false));
    }


    @Test
    public void testDeleteNode() throws Exception {
        ZooKeeperClient zk1 = buildZooKeeper();
        zk1.createPersistentNode("/tests/parent-node");
        zk1.createPersistentNode("/tests/parent-node/child-node1");
        zk1.createPersistentNode("/tests/parent-node/child-node2");
        zk1.createPersistentNode("/tests/parent-node/child-node3");
        zk1.createPersistentNode("/tests/parent-node/child-node1/child-node4");
        zk1.createPersistentNode("/tests/parent-node/child-node1/child-node5");
        // Make sure node exist
        assertThat(zk1.getNode("/tests/parent-node/child-node1/child-node4", null), notNullValue());
        assertThat(zk1.getNode("/tests/parent-node", null), notNullValue());

        try {
            zk1.deleteNode("/tests/parent-node");
            assertThat("Shouldn't delete non-empty node by default", false);
        } catch (ZooKeeperClientException ex) {
            // Ignore
        }

        zk1.deleteNodeRecursively("/tests/parent-node");
        assertThat(zk1.getNode("/tests/parent-node/child-node1/child-node3", null), nullValue());
        assertThat(zk1.getNode("/tests/parent-node", null), nullValue());
        assertThat(zk1.getNode("/tests", null), notNullValue());
    }

    @Test
    public void testLargeSequentialNode() throws Exception {
        byte[] largeData = new byte[1024 * 1024 * 10];

        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 119);
        }

        ZooKeeperClient zk1 = buildZooKeeper();
        String path = zk1.createLargeSequentialNode("/tests/large-node_", largeData);
        try {
            byte[] largeDataCreated = zk1.getLargeNode(path);
            assertThat(largeDataCreated.length, equalTo(largeData.length));
            for (int i = 0; i < largeDataCreated.length; i++) {
                assertThat(largeDataCreated[i], equalTo(largeData[i]));
            }
        } finally {
            zk1.deleteLargeNode(path);
        }
    }


    @Test public void testLargeSequentialNodeWithVariableSize() throws Exception {
        byte[] largeData = new byte[1024 * 1024 * 10];

        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 119);
        }

        ZooKeeperClient zk1 = buildZooKeeper(ImmutableSettings.settingsBuilder().put("zookeeper.maxnodesize", 1000000).build());
        String path = zk1.createLargeSequentialNode("/tests/large-node_", largeData);

        ZooKeeperClient zk2 = buildZooKeeper(ImmutableSettings.settingsBuilder().put("zookeeper.maxnodesize", 100000).build());
        byte[] largeDataCreated = zk2.getLargeNode(path);
        assertThat(largeDataCreated.length, equalTo(largeData.length));
        for (int i = 0; i < largeDataCreated.length; i++) {
            assertThat(largeDataCreated[i], equalTo(largeData[i]));
        }
        zk2.deleteLargeNode(path);
    }
}
