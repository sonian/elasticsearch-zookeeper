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

package com.sonian.elasticsearch.zookeeper.discovery.embedded;

import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.env.Environment;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;

/**
 * @author imotov
 */
public class EmbeddedZooKeeperService extends AbstractLifecycleComponent<EmbeddedZooKeeper> implements EmbeddedZooKeeper {
    private ZooKeeperServer zooKeeperServer;

    private NIOServerCnxn.Factory cnxnFactory;

    public EmbeddedZooKeeperService(Settings settings, Environment environment) {
        super(settings);
        try {
            zooKeeperServer = new ZooKeeperServer();
            File zooKeeperDir = new File(environment.dataFiles()[0], "zookeeper");
            FileTxnSnapLog fileTxnSnapLog = new FileTxnSnapLog(zooKeeperDir, zooKeeperDir);
            zooKeeperServer.setTxnLogFactory(fileTxnSnapLog);
            zooKeeperServer.setTickTime(ZooKeeperServer.DEFAULT_TICK_TIME);
            // Large session timeout so it doesn't time out during debugging
            zooKeeperServer.setMinSessionTimeout(100000);
            zooKeeperServer.setMaxSessionTimeout(100000);
            String zooKeeperPort = settings.get("zookeeper.port", "2800-2900");
            PortsRange portsRange = new PortsRange(zooKeeperPort);
            for (int port : portsRange.ports()) {
                InetSocketAddress address = new InetSocketAddress(port);
                try {
                    cnxnFactory = new NIOServerCnxn.Factory(address, -1);
                    zooKeeperServer.setServerCnxnFactory(cnxnFactory);
                    break;
                } catch (BindException bindException) {
                    // Ignore
                }
            }
        } catch (Exception ex) {
            logger.error("ZooKeeper initialization failed ", ex);
        }

    }

    @Override protected void doStart() throws ElasticsearchException {
        try {
            cnxnFactory.startup(zooKeeperServer);
        } catch (IOException e) {
            throw new ElasticsearchException("Cannot start ZooKeeper", e);
        } catch (InterruptedException e) {
            throw new ElasticsearchException("ZooKeeper startup interrupted", e);
        }
    }

    @Override protected void doStop() throws ElasticsearchException {
        cnxnFactory.shutdown();
    }

    @Override protected void doClose() throws ElasticsearchException {
    }

    @Override public int port() {
        return zooKeeperServer.getClientPort();
    }

    @Override public void expireSession(long sessionId) {
        logger.info("Expiring session {}", sessionId);
        zooKeeperServer.closeSession(sessionId);
    }
}
