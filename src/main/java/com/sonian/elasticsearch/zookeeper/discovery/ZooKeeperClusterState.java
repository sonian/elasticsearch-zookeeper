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

import com.sonian.elasticsearch.zookeeper.client.*;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationExplanation;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.DiscoveryNodesProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Publishes and retrieves cluster state using zookeeper.
 * <p/>
 * By default cluster state is stored in /es/elasticsearch/state node. State is published in several parts.
 * Each part is updated only if it got changed and then /es/elasticsearch/state/state node is updated to
 * reflect the latest versions of the parts.
 *
 * @author imotov
 */
public class ZooKeeperClusterState extends AbstractLifecycleComponent<ZooKeeperClusterState> {

    private final Lock publishingLock = new ReentrantLock();

    private final ZooKeeperClient zooKeeperClient;

    private final ZooKeeperEnvironment environment;

    private final List<ClusterStatePart<?>> parts = new ArrayList<ClusterStatePart<?>>();

    private final DiscoveryNodesProvider nodesProvider;

    private final String clusterStateVersion = Version.CURRENT.number();

    public ZooKeeperClusterState(Settings settings, ZooKeeperEnvironment environment, ZooKeeperClient zooKeeperClient, DiscoveryNodesProvider nodesProvider) {
        super(settings);
        this.zooKeeperClient = zooKeeperClient;
        this.environment = environment;
        this.nodesProvider = nodesProvider;
        initClusterStatePersistence();
    }


    /**
     * Publishes new cluster state
     *
     * @param state
     * @throws org.elasticsearch.ElasticSearchException
     *
     * @throws InterruptedException
     */
    public void publish(ClusterState state) throws ElasticSearchException, InterruptedException {
        publishingLock.lock();
        try {
            logger.trace("Publishing new cluster state");
            final String statePath = environment.statePartsNodePath();
            final BytesStreamOutput buf = new BytesStreamOutput();
            buf.writeUTF(clusterStateVersion());
            buf.writeLong(state.version());
            for (ClusterStatePart<?> part : this.parts) {
                buf.writeUTF(part.publishClusterStatePart(state));
            }
            zooKeeperClient.setOrCreatePersistentNode(statePath, buf.copiedByteArray());

            // Cleaning previous versions of updated cluster parts
            for (ClusterStatePart<?> part : this.parts) {
                part.purge();
            }
        } catch (IOException e) {
            throw new ZooKeeperClientException("Cannot publish state", e);
        } finally {
            publishingLock.unlock();
        }

    }

    /**
     * Retrieves cluster state
     *
     * @param newClusterStateListener triggered when cluster state changes
     * @return
     * @throws ElasticSearchException
     * @throws InterruptedException
     */
    public ClusterState retrieve(final NewClusterStateListener newClusterStateListener) throws ElasticSearchException, InterruptedException {
        publishingLock.lock();
        try {
            if (!lifecycle.started()) {
                return null;
            }
            logger.trace("Retrieving new cluster state");

            final String statePath = environment.statePartsNodePath();
            ZooKeeperClient.NodeListener nodeListener;
            if (newClusterStateListener != null) {
                nodeListener = new AbstractNodeListener() {
                    @Override
                    public void onNodeCreated(String id) {
                        updateClusterState(newClusterStateListener);
                    }

                    @Override
                    public void onNodeDataChanged(String id) {
                        updateClusterState(newClusterStateListener);
                    }
                };
            } else {
                nodeListener = null;
            }
            byte[] stateBuf = zooKeeperClient.getNode(statePath, nodeListener);
            if (stateBuf == null) {
                return null;
            }
            final BytesStreamInput buf = new BytesStreamInput(stateBuf);
            String clusterStateVersion = buf.readUTF();
            if (!clusterStateVersion().equals(clusterStateVersion)) {
                throw new ZooKeeperIncompatibleStateVersionException("Expected: " + clusterStateVersion() + ", actual: " + clusterStateVersion);
            }

            ClusterState.Builder builder = ClusterState.newClusterStateBuilder()
                    .version(buf.readLong());
            for (ClusterStatePart<?> part : this.parts) {
                builder = part.set(builder, buf.readUTF());
                if (builder == null) {
                    return null;
                }
            }

            return builder.build();
        } catch (IOException e) {
            throw new ZooKeeperClientException("Cannot retrieve state", e);
        } finally {
            publishingLock.unlock();
        }

    }

    /**
     * Makes sure that internal cache structures are in sync with zookeeper
     * <p/>
     * This method should be called when node becomes master and switches from retrieving cluster state
     * to publishing cluster state.
     */
    public void syncClusterState() throws ElasticSearchException, InterruptedException {
        // To prepare for publishing master state, make sure that we are in sync with zooKeeper
        try {
            retrieve(null);
        } catch (ZooKeeperIncompatibleStateVersionException ex) {
            logger.info("Incompatible version of state found - cleaning. {}", ex.getMessage());
            cleanClusterStateNode();
        }
    }

    private void cleanClusterStateNode() throws ElasticSearchException, InterruptedException {
        zooKeeperClient.deleteNodeRecursively(environment.stateNodePath());
    }

    private void updateClusterState(NewClusterStateListener newClusterStateListener) {
        try {
            ClusterState clusterState = retrieve(newClusterStateListener);
            if (clusterState != null) {
                newClusterStateListener.onNewClusterState(clusterState);
            }
        } catch (ZooKeeperClientSessionExpiredException ex) {
            // Ignore session should be restarted
        } catch (Exception ex) {
            logger.error("Error updating cluster state", ex);
        }
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        try {
            zooKeeperClient.createPersistentNode(environment.stateNodePath());
        } catch (InterruptedException ex) {
            // Ignore
        }
    }

    @Override
    protected void doStop() throws ElasticSearchException {
    }

    @Override
    protected void doClose() throws ElasticSearchException {
    }

    protected String clusterStateVersion() {
        return clusterStateVersion;
    }


    public interface NewClusterStateListener {
        public void onNewClusterState(ClusterState clusterState);
    }

    // TODO: this logic should be moved to the actual classes that represent parts of Cluster State after zookeeper-
    // based discovery is merged to master.
    private void initClusterStatePersistence() {
        parts.add(new ClusterStatePart<RoutingTable>("routingTable") {
            @Override
            public void writeTo(RoutingTable statePart, StreamOutput out) throws IOException {
                RoutingTable.Builder.writeTo(statePart, out);
            }

            @Override
            public RoutingTable readFrom(StreamInput in) throws IOException {
                return RoutingTable.Builder.readFrom(in);
            }

            @Override
            public RoutingTable get(ClusterState state) {
                return state.getRoutingTable();
            }

            @Override
            public ClusterState.Builder set(ClusterState.Builder builder, RoutingTable val) {
                return builder.routingTable(val);
            }
        });
        parts.add(new ClusterStatePart<DiscoveryNodes>("discoveryNodes") {
            @Override
            public void writeTo(DiscoveryNodes statePart, StreamOutput out) throws IOException {
                DiscoveryNodes.Builder.writeTo(statePart, out);
            }

            @Override
            public DiscoveryNodes readFrom(StreamInput in) throws IOException {
                return DiscoveryNodes.Builder.readFrom(in, nodesProvider.nodes().localNode());
            }

            @Override
            public DiscoveryNodes get(ClusterState state) {
                return state.getNodes();
            }

            @Override
            public ClusterState.Builder set(ClusterState.Builder builder, DiscoveryNodes val) {
                return builder.nodes(val);
            }
        });
        parts.add(new ClusterStatePart<MetaData>("metaData") {
            @Override
            public void writeTo(MetaData statePart, StreamOutput out) throws IOException {
                MetaData.Builder.writeTo(statePart, out);
            }

            @Override
            public MetaData readFrom(StreamInput in) throws IOException {
                return MetaData.Builder.readFrom(in);
            }

            @Override
            public MetaData get(ClusterState state) {
                return state.metaData();
            }

            @Override
            public ClusterState.Builder set(ClusterState.Builder builder, MetaData val) {
                return builder.metaData(val);
            }
        });
        parts.add(new ClusterStatePart<ClusterBlocks>("clusterBlocks") {
            @Override
            public void writeTo(ClusterBlocks statePart, StreamOutput out) throws IOException {
                ClusterBlocks.Builder.writeClusterBlocks(statePart, out);
            }

            @Override
            public ClusterBlocks readFrom(StreamInput in) throws IOException {
                return ClusterBlocks.Builder.readClusterBlocks(in);
            }

            @Override
            public ClusterBlocks get(ClusterState state) {
                return state.blocks();
            }

            @Override
            public ClusterState.Builder set(ClusterState.Builder builder, ClusterBlocks val) {
                return builder.blocks(val);
            }
        });
        parts.add(new ClusterStatePart<AllocationExplanation>("allocationExplanation") {
            @Override
            public void writeTo(AllocationExplanation statePart, StreamOutput out) throws IOException {
                statePart.writeTo(out);
            }

            @Override
            public AllocationExplanation readFrom(StreamInput in) throws IOException {
                return AllocationExplanation.readAllocationExplanation(in);
            }

            @Override
            public AllocationExplanation get(ClusterState state) {
                return state.allocationExplanation();
            }

            @Override
            public ClusterState.Builder set(ClusterState.Builder builder, AllocationExplanation val) {
                return builder.allocationExplanation(val);
            }
        });
    }

    private abstract class ClusterStatePart<T> {
        private final String statePartName;

        private T cached;

        private String cachedPath;

        private String previousPath;

        public ClusterStatePart(String statePartName) {
            this.statePartName = statePartName;
        }

        public String publishClusterStatePart(ClusterState state) throws ElasticSearchException, InterruptedException {
            T statePart = get(state);
            if (statePart.equals(cached)) {
                return cachedPath;
            } else {
                String path = internalPublishClusterStatePart(statePart);
                cached = statePart;
                previousPath = cachedPath;
                cachedPath = path;
                return path;
            }
        }

        private String internalPublishClusterStatePart(T statePart) throws ElasticSearchException, InterruptedException {
            final String path = environment.stateNodePath() + "/" + statePartName + "_";
            String rootPath;
            try {
                BytesStreamOutput streamOutput = new BytesStreamOutput();
                writeTo(statePart, streamOutput);
                // Create Root node with version and size of the state part
                rootPath = zooKeeperClient.createLargeSequentialNode(path, streamOutput.copiedByteArray());
            } catch (IOException e) {
                throw new ZooKeeperClientException("Cannot read " + statePartName + " node at " + path, e);
            }
            return rootPath;
        }

        public T getClusterStatePart(String path) throws ElasticSearchException, InterruptedException {
            if (path.equals(cachedPath)) {
                return cached;
            } else {
                T part = internalGetStatePart(path);
                if (part != null) {
                    cached = part;
                    cachedPath = path;
                    return cached;
                } else {
                    return null;
                }
            }

        }

        public void purge() throws ElasticSearchException, InterruptedException {
            if (previousPath != null) {
                zooKeeperClient.deleteLargeNode(previousPath);
                previousPath = null;
            }
        }

        public T internalGetStatePart(final String path) throws ElasticSearchException, InterruptedException {
            try {

                byte[] buf = zooKeeperClient.getLargeNode(path);
                if (buf != null) {
                    return readFrom(new BytesStreamInput(buf));
                } else {
                    return null;
                }
            } catch (IOException e) {
                throw new ZooKeeperClientException("Cannot read " + statePartName + " node at " + path, e);
            }
        }

        public ClusterState.Builder set(ClusterState.Builder builder, String path) throws ElasticSearchException, InterruptedException {
            T val = getClusterStatePart(path);
            if (val == null) {
                return null;
            } else {
                return set(builder, val);
            }

        }

        public abstract void writeTo(T statePart, StreamOutput out) throws IOException;

        public abstract T readFrom(StreamInput in) throws IOException;

        public abstract T get(ClusterState state);

        public abstract ClusterState.Builder set(ClusterState.Builder builder, T val);

    }

}
