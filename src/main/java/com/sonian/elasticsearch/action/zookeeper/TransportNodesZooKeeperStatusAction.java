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

package com.sonian.elasticsearch.action.zookeeper;

import com.sonian.elasticsearch.zookeeper.discovery.ZooKeeperDiscovery;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.support.nodes.NodeOperationRequest;
import org.elasticsearch.action.support.nodes.TransportNodesOperationAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;


/**
 */
public class TransportNodesZooKeeperStatusAction extends
        TransportNodesOperationAction<NodesZooKeeperStatusRequest, NodesZooKeeperStatusResponse,
                TransportNodesZooKeeperStatusAction.NodeZooKeeperStatusRequest,
                NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse> {
    private final ZooKeeperDiscovery zooKeeperDiscovery;


    @Inject
    public TransportNodesZooKeeperStatusAction(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                                ClusterService clusterService, TransportService transportService,
                                                Discovery discovery) {
        super(settings, clusterName, threadPool, clusterService, transportService);
        if(discovery instanceof ZooKeeperDiscovery) {
            zooKeeperDiscovery = (ZooKeeperDiscovery) discovery;
        } else {
            zooKeeperDiscovery = null;
        }
    }

    @Override
    protected String transportAction() {
        return "/zookeeper/settings/get";
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected NodesZooKeeperStatusRequest newRequest() {
        return new NodesZooKeeperStatusRequest();
    }

    @Override
    protected NodesZooKeeperStatusResponse newResponse(NodesZooKeeperStatusRequest nodesZooKeeperStatusRequest, AtomicReferenceArray responses) {
        final List<NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse> nodeZooKeeperStatusResponses = Lists.newArrayList();
        for (int i = 0; i < responses.length(); i++) {
            Object resp = responses.get(i);
            if (resp instanceof NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse) {
                nodeZooKeeperStatusResponses.add((NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse) resp);
            }
        }
        return new NodesZooKeeperStatusResponse(
                clusterName, nodeZooKeeperStatusResponses.toArray(
                new NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse[nodeZooKeeperStatusResponses.size()]));
    }

    @Override
    protected NodeZooKeeperStatusRequest newNodeRequest() {
        return new NodeZooKeeperStatusRequest();
    }

    @Override
    protected NodeZooKeeperStatusRequest newNodeRequest(String nodeId, NodesZooKeeperStatusRequest nodesZooKeeperStatusRequest) {
        return new NodeZooKeeperStatusRequest(nodesZooKeeperStatusRequest, nodeId);
    }

    @Override
    protected NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse newNodeResponse() {
        return new NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse();
    }

    @Override
    protected NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse nodeOperation(NodeZooKeeperStatusRequest nodeZooKeeperStatusRequest) throws ElasticSearchException {
        if (zooKeeperDiscovery != null) {
            try {
                return new NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse(
                        clusterService.state().nodes().localNode(), true,
                        zooKeeperDiscovery.verifyConnection(nodeZooKeeperStatusRequest.timeout()));
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
        return new NodesZooKeeperStatusResponse.NodeZooKeeperStatusResponse(
                clusterService.state().nodes().localNode(), false, false);
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
    }

    public static class NodeZooKeeperStatusRequest extends NodeOperationRequest {

        private TimeValue zooKeeperTimeout;

        private NodeZooKeeperStatusRequest() {

        }

        private NodeZooKeeperStatusRequest(NodesZooKeeperStatusRequest request, String nodeId) {
            super(request, nodeId);
            zooKeeperTimeout = request.zooKeeperTimeout();
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            zooKeeperTimeout = TimeValue.readTimeValue(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            zooKeeperTimeout.writeTo(out);
        }

        public TimeValue timeout() {
            return zooKeeperTimeout;
        }
    }
}
