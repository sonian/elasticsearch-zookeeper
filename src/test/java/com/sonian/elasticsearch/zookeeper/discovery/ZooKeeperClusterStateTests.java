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

import com.sonian.elasticsearch.zookeeper.client.ZooKeeperClient;
import com.sonian.elasticsearch.zookeeper.client.ZooKeeperIncompatibleStateVersionException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author imotov
 */
public class ZooKeeperClusterStateTests extends AbstractZooKeeperTests {
    @BeforeClass
    public void createTestPaths() throws Exception {
        startZooKeeper();
    }

    @AfterClass
    public void shutdownZooKeeper() {
        stopZooKeeper();
    }

    public ZooKeeperClusterStateTests() {
        putDefaultSettings(ImmutableSettings.settingsBuilder().put("zookeeper.maxnodesize", 10).build());
    }

    @Test
    public void testClusterStatePublishing() throws Exception {

        RoutingTable routingTable = testRoutingTable();
        DiscoveryNodes nodes = testDiscoveryNodes();
        ClusterState initialState = testClusterState(routingTable, nodes);
        ZooKeeperClusterState zkState = buildZooKeeperClusterState(nodes);

        zkState.start();

        zkState.publish(initialState);

        final CountDownLatch latch = new CountDownLatch(1);

        ClusterState retrievedState = zkState.retrieve(new ZooKeeperClusterState.NewClusterStateListener() {

            @Override
            public void onNewClusterState(ClusterState clusterState) {
                latch.countDown();
            }
        });

        assertThat(ClusterState.Builder.toBytes(retrievedState),
                equalTo(ClusterState.Builder.toBytes(initialState)));

        ClusterState secondVersion = ClusterState.builder(initialState)
                .version(1235L)
                .build();

        zkState.publish(secondVersion);

        retrievedState = zkState.retrieve(null);

        assertThat(ClusterState.Builder.toBytes(retrievedState),
                equalTo(ClusterState.Builder.toBytes(secondVersion)));

        assertThat(latch.await(1, TimeUnit.SECONDS), equalTo(true));
        zkState.stop();

    }

    @Test
    public void testClusterStatePublishingWithNewVersion() throws Exception {
        RoutingTable routingTable = testRoutingTable();
        DiscoveryNodes nodes = testDiscoveryNodes();
        ClusterState initialState = testClusterState(routingTable, nodes);

        ZooKeeperClusterState zkStateOld = buildZooKeeperClusterState(nodes, "0.0.1");

        zkStateOld.start();

        zkStateOld.publish(initialState);

        zkStateOld.stop();

        ZooKeeperClusterState zkStateNew = buildZooKeeperClusterState(nodes, "0.0.2");

        zkStateNew.start();

        try {
            zkStateNew.retrieve(null);
            assertThat("Shouldn't read the state stored by a different version", false);
        } catch (ZooKeeperIncompatibleStateVersionException ex) {
            assertThat(ex.getMessage(), containsString("0.0.1"));
            assertThat(ex.getMessage(), containsString("0.0.2"));
        }
        ZooKeeperClient zk = buildZooKeeper();

        // Make sure that old state wasn't deleted
        assertThat(zk.getNode(zooKeeperEnvironment().statePartsNodePath(), null), notNullValue());

        zkStateNew.syncClusterState();

        // Make sure that new start can be published now
        zkStateNew.publish(initialState);

        zkStateNew.stop();

    }

}
