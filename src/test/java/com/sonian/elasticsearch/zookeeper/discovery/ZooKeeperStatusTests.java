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

import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.node.internal.InternalNode;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

/**
 */
public class ZooKeeperStatusTests extends AbstractZooKeeperNodeTests {

    @Test
    public void testZooKeeperConnection() throws Exception {
        startZooKeeper();
        buildNode("node1");
        node("node1").start();
        InternalNode node = (InternalNode) node("node1");
        HttpServerTransport transport = node.injector().getInstance(HttpServerTransport.class);
        InetSocketAddress address = ((InetSocketTransportAddress) transport.boundAddress().publishAddress()).address();
        URL url = new URL("http", address.getHostName(), address.getPort(), "/_zookeeper/status?timeout=1s");
        assertThat(getUrlContent(url), containsString("\"connected\":true"));
        stopZooKeeper();
        assertThat(getUrlContent(url), containsString("\"connected\":false"));
    }

    @Test
    public void testZooKeeperConnectionTwoNodes() throws Exception {
        startZooKeeper();
        buildNode("node1");
        node("node1").start();
        buildNode("node2");
        node("node2").start();
        InternalNode node = (InternalNode) node("node1");
        HttpServerTransport transport = node.injector().getInstance(HttpServerTransport.class);
        InetSocketAddress address = ((InetSocketTransportAddress) transport.boundAddress().publishAddress()).address();
        URL url = new URL("http", address.getHostName(), address.getPort(), "/_zookeeper/status/node2?timeout=1s");
        assertThat(getUrlContent(url), containsString("{\"name\":\"node2\",\"enabled\":true,\"connected\":true}"));
    }

    @Test
    public void testZooKeeperConnectionWithZooKeeperDisabled() throws Exception {
        buildNode("node1", ImmutableSettings.settingsBuilder()
                .put("sonian.elasticsearch.zookeeper.discovery.state_publishing.enabled", false)
                .put("sonian.elasticsearch.zookeeper.settings.enabled", false)
                .put("discovery.type", "local")
        );
        node("node1").start();
        InternalNode node = (InternalNode) node("node1");
        HttpServerTransport transport = node.injector().getInstance(HttpServerTransport.class);
        InetSocketAddress address = ((InetSocketTransportAddress) transport.boundAddress().publishAddress()).address();
        URL url = new URL("http", address.getHostName(), address.getPort(), "/_zookeeper/status?timeout=1s");
        assertThat(getUrlContent(url), containsString("\"enabled\":false"));
    }


    public String getUrlContent(URL url) throws IOException {
        InputStream responseStream = (InputStream) url.getContent();
        ByteArrayOutputStream tempStream = new ByteArrayOutputStream();
        Streams.copy(responseStream, tempStream);
        responseStream.close();
        return tempStream.toString();
    }

}
