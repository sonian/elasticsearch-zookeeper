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

import org.elasticsearch.action.support.nodes.NodesOperationRequest;
import org.elasticsearch.common.unit.TimeValue;

/**
 */
public class NodesZooKeeperStatusRequest extends NodesOperationRequest<NodesZooKeeperStatusRequest> {

    private TimeValue zooKeeperTimeout = TimeValue.timeValueSeconds(10);

    public NodesZooKeeperStatusRequest() {

    }

    public NodesZooKeeperStatusRequest(String... nodeIds) {
        super(nodeIds);
    }

    public NodesZooKeeperStatusRequest zooKeeperTimeout(TimeValue timeout) {
        this.zooKeeperTimeout = timeout;
        return this;
    }

    public TimeValue zooKeeperTimeout() {
        return zooKeeperTimeout;
    }
}
