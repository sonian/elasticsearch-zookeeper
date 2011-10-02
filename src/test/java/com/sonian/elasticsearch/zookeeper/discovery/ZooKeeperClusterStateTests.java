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

import com.sonian.elasticsearch.zookeeper.discovery.ZooKeeperClusterState;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ImmutableShardRouting;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.discovery.zen.DiscoveryNodesProvider;
import com.sonian.elasticsearch.zookeeper.client.ZooKeeperClient;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author imotov
 */
public class ZooKeeperClusterStateTests extends AbstractZooKeeperTests {

    @BeforeClass public void createTestPaths() throws Exception {
        buildZooKeeper().createPersistentNode("/es/elasticsearch/state");
    }


    ZooKeeperClusterState buildZooKeeperClusterState(DiscoveryNodesProvider provider) {
        ZooKeeperClient zk = buildZooKeeper(ImmutableSettings.settingsBuilder()
                .put("zookeeper.maxnodesize", 10)
                .build());

        return new ZooKeeperClusterState(defaultSettings(),
                zooKeeperEnvironment(),
                zk,
                provider
        );
    }

    @Test public void testClusterStatePublishing() throws Exception {

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        for (int i = 0; i < 1000; i++) {
            IndexRoutingTable.Builder indexRoutingTableBuilder = new IndexRoutingTable.Builder("index");
            for (int j = 0; j < 100; j++) {
                ShardRouting shardRouting = new ImmutableShardRouting("index", j, "i" + i + "s" + j, true, ShardRoutingState.STARTED, 0L);
                indexRoutingTableBuilder.addShard(shardRouting, true);
            }
            routingTableBuilder.add(indexRoutingTableBuilder);
        }

        RoutingTable routingTable = routingTableBuilder
                .build();

        final DiscoveryNodes nodes = DiscoveryNodes.newNodesBuilder()
                .masterNodeId("localnodeid")
                .build();

        ClusterState initialState = ClusterState.newClusterStateBuilder()
                .version(1234L)
                .routingTable(routingTable)
                .nodes(nodes)
                .build();

        ZooKeeperClusterState zkState = buildZooKeeperClusterState(new DiscoveryNodesProvider() {
            @Override public DiscoveryNodes nodes() {
                return nodes;  //To change body of implemented methods use File | Settings | File Templates.
            }
        });
        zkState.start();

        zkState.publish(initialState);

        final CountDownLatch latch = new CountDownLatch(1);

        ClusterState retrievedState = zkState.retrieve(new ZooKeeperClusterState.NewClusterStateListener() {

            @Override public void onNewClusterState(ClusterState clusterState) {
                latch.countDown();
            }
        });

        assertThat(ClusterState.Builder.toBytes(retrievedState),
                equalTo(ClusterState.Builder.toBytes(initialState)));

        ClusterState secondVersion = ClusterState.newClusterStateBuilder()
                .state(initialState)
                .version(1235L)
                .build();

        zkState.publish(secondVersion);

        retrievedState = zkState.retrieve(null);

        assertThat(ClusterState.Builder.toBytes(retrievedState),
                equalTo(ClusterState.Builder.toBytes(secondVersion)));

        assertThat(latch.await(1, TimeUnit.SECONDS), equalTo(true));
        zkState.stop();

    }
}
