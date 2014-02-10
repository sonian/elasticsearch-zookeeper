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
import com.sonian.elasticsearch.zookeeper.client.ZooKeeperClientService;
import com.sonian.elasticsearch.zookeeper.client.ZooKeeperEnvironment;
import com.sonian.elasticsearch.zookeeper.client.ZooKeeperFactory;
import com.sonian.elasticsearch.zookeeper.discovery.embedded.EmbeddedZooKeeperService;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.DiscoveryNodesProvider;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.service.NodeService;
import org.testng.annotations.AfterMethod;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 * @author imotov
 */
public abstract class AbstractZooKeeperTests {

    protected final ESLogger logger = Loggers.getLogger(getClass());

    protected EmbeddedZooKeeperService embeddedZooKeeperService;

    protected final List<ZooKeeperClient> zooKeeperClients = new ArrayList<ZooKeeperClient>();

    private Settings defaultSettings = ImmutableSettings
            .settingsBuilder()
            .put("cluster.name", "test-cluster-" + NetworkUtils.getLocalAddress().getHostName())
            .build();

    private ZooKeeperEnvironment environment;

    private ZooKeeperFactory zooKeeperFactory;

    public void putDefaultSettings(Settings.Builder settings) {
        putDefaultSettings(settings.build());
    }

    public void putDefaultSettings(Settings settings) {
        defaultSettings = ImmutableSettings.settingsBuilder().put(defaultSettings).put(settings).build();
    }


    public void startZooKeeper() throws IOException, InterruptedException {
        startZooKeeper(true);
    }

    public void startZooKeeper(boolean cleanDirectory) throws IOException, InterruptedException {
        Environment tempEnvironment = new Environment(defaultSettings);
        if (cleanDirectory) {
            File zooKeeperDataDirectory = new File(tempEnvironment.dataFiles()[0], "zookeeper");
            logger.info("Deleting zookeeper directory {}", zooKeeperDataDirectory);
            if (deleteDirectory(zooKeeperDataDirectory)) {
                logger.info("Zookeeper directory {} was deleted", zooKeeperDataDirectory);
            }
        }
        embeddedZooKeeperService = new EmbeddedZooKeeperService(defaultSettings, tempEnvironment);
        embeddedZooKeeperService.start();
        putDefaultSettings(ImmutableSettings.settingsBuilder()
                .put(defaultSettings)
                .put("sonian.elasticsearch.zookeeper.client.host", "localhost:" + embeddedZooKeeperService.port()));

        zooKeeperFactory = new ZooKeeperFactory(defaultSettings);
        environment = new ZooKeeperEnvironment(defaultSettings, ClusterName.clusterNameFromSettings(defaultSettings));
    }

    public void stopZooKeeper() {
        if (embeddedZooKeeperService != null) {
            embeddedZooKeeperService.stop();
            embeddedZooKeeperService.close();
            embeddedZooKeeperService = null;
        }
    }

    public void restartZooKeeper() throws IOException, InterruptedException {
        logger.info("*** ZOOKEEPER RESTART ***");
        stopZooKeeper();
        Thread.sleep(1000);
        startZooKeeper(false);
    }

    @AfterMethod
    public void stopZooKeeperClients() {
        for (ZooKeeperClient zooKeeperClient : zooKeeperClients) {
            logger.info("Closing {}" + zooKeeperClient);
            zooKeeperClient.stop();
            zooKeeperClient.close();
        }
        zooKeeperClients.clear();
    }

    public ZooKeeperClient buildZooKeeper() {
        return buildZooKeeper(ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    public ZooKeeperClient buildZooKeeper(Settings settings) {
        String settingsSource = getClass().getName().replace('.', '/') + ".yml";
        Settings finalSettings = settingsBuilder()
                .loadFromClasspath(settingsSource)
                .put(defaultSettings)
                .put(settings)
                .build();
        ZooKeeperEnvironment environment = new ZooKeeperEnvironment(finalSettings, ClusterName.clusterNameFromSettings(defaultSettings));
        ZooKeeperClient zooKeeperClient = new ZooKeeperClientService(finalSettings, environment, zooKeeperFactory);
        zooKeeperClient.start();
        zooKeeperClients.add(zooKeeperClient);
        return zooKeeperClient;
    }

    public ZooKeeperFactory zooKeeperFactory() {
        return zooKeeperFactory;
    }

    public ZooKeeperEnvironment zooKeeperEnvironment() {
        return environment;
    }

    public Settings defaultSettings() {
        return defaultSettings;
    }


    private boolean deleteDirectory(File path) {
        if (path.exists()) {
            File[] files = path.listFiles();
            for (File file : files) {
                if (file.isDirectory()) {
                    if (!deleteDirectory(file)) {
                        return false;
                    }
                } else {
                    if (!file.delete()) {
                        return false;
                    }
                }
            }
            return path.delete();
        }
        return false;
    }


    protected ClusterState testClusterState(RoutingTable routingTable, DiscoveryNodes nodes) {
        return ClusterState.builder()
                .version(1234L)
                .routingTable(routingTable)
                .nodes(nodes)
                .build();
    }

    protected DiscoveryNodes testDiscoveryNodes() {
        return DiscoveryNodes.builder()
                .masterNodeId("localnodeid")
                .build();
    }

    protected RoutingTable testRoutingTable() {
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        for (int i = 0; i < 1000; i++) {
            IndexRoutingTable.Builder indexRoutingTableBuilder = new IndexRoutingTable.Builder("index");
            for (int j = 0; j < 100; j++) {
                ShardId shardId = new ShardId("index", j);
                IndexShardRoutingTable.Builder indexShardRoutingTableBuilder = new IndexShardRoutingTable.Builder(shardId, true);
                ImmutableShardRouting shardRouting = new ImmutableShardRouting("index", j, "i" + i + "s" + j, true, ShardRoutingState.STARTED, 0L);
                indexShardRoutingTableBuilder.addShard(shardRouting);
                indexRoutingTableBuilder.addShard(indexShardRoutingTableBuilder.build(), shardRouting);
            }
            routingTableBuilder.add(indexRoutingTableBuilder);
        }

        return routingTableBuilder
                .build();
    }

    protected ZooKeeperClusterState buildZooKeeperClusterState(DiscoveryNodes nodes) {
        return buildZooKeeperClusterState(nodes, null);
    }

    protected ZooKeeperClusterState buildZooKeeperClusterState(final DiscoveryNodes nodes, String clusterStateVersion) {
        DiscoveryNodesProvider provider = new DiscoveryNodesProvider() {
            @Override
            public DiscoveryNodes nodes() {
                return nodes;
            }

            @Override
            public NodeService nodeService() {
                return null;
            }
        };
        ZooKeeperClient zk = buildZooKeeper(defaultSettings());
        if (clusterStateVersion != null) {
            return new ZooKeeperClusterStateVersionOverride(clusterStateVersion, defaultSettings(), zooKeeperEnvironment(),
                    zk,
                    provider
            );
        } else {
            return new ZooKeeperClusterState(defaultSettings(),
                    zooKeeperEnvironment(),
                    zk,
                    provider
            );
        }
    }

    private class ZooKeeperClusterStateVersionOverride extends ZooKeeperClusterState {

        private final String clusterStateVersion;

        public ZooKeeperClusterStateVersionOverride(String clusterStateVersion, Settings settings,
                                                    ZooKeeperEnvironment environment, ZooKeeperClient zooKeeperClient,
                                                    DiscoveryNodesProvider nodesProvider) {
            super(settings, environment, zooKeeperClient, nodesProvider);
            this.clusterStateVersion = clusterStateVersion;
        }

        @Override
        protected String clusterStateVersion() {
            return clusterStateVersion;
        }
    }

}
