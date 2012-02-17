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

package com.sonian.elasticsearch.rest.zookeeper;

import com.sonian.elasticsearch.zookeeper.client.ZooKeeperClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

/**
 */
public class RestZooKeeperStatusAction extends BaseRestHandler {

    private final ZooKeeperClient zooKeeperClient;

    @Inject
    public RestZooKeeperStatusAction(Settings settings, Client client, RestController controller, ZooKeeperClient zooKeeperClient) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.GET, "/_zookeeper/status", this);
        this.zooKeeperClient = zooKeeperClient;
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel) {
        try {
            XContentBuilder builder = restContentBuilder(request);
            builder.startObject();
            long timeout = request.paramAsLong("timeout", 10);
            boolean connected = zooKeeperClient.verifyConnection(timeout, TimeUnit.SECONDS);
            builder.field("connected", connected);
            builder.endObject();
            channel.sendResponse(new XContentRestResponse(request, RestStatus.OK, builder));
        } catch (Exception ex) {
            try {
                channel.sendResponse(new XContentThrowableRestResponse(request, ex));
            } catch (IOException e) {
                logger.warn("Error sending zookeeper status", e);
            }
        }
    }
}
