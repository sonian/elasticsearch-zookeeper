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


package com.sonian.elasticsearch.zookeeper.client;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author imotov
 */
public class ZooKeeperFactory extends AbstractComponent {

    private final String host;

    private final TimeValue sessionTimeout;

    @Inject public ZooKeeperFactory(Settings settings) {
        super(settings);
        host = componentSettings.get("host");
        if (host == null) {
            throw new ElasticSearchException("Empty ZooKeeper host name");
        }
        sessionTimeout = componentSettings.getAsTime("session.timeout", new TimeValue(1, TimeUnit.MINUTES));
    }

    public ZooKeeper newZooKeeper() {
        return newZooKeeper(new Watcher() {
            @Override public void process(WatchedEvent event) {
            }
        });
    }

    public ZooKeeper newZooKeeper(Watcher watcher) {
        try {
            return new ZooKeeper(host, (int) sessionTimeout.millis(), watcher);
        } catch (IOException e) {
            throw new ElasticSearchException("Cannot start ZooKeeper", e);
        }
    }

}
