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

package com.sonian.elasticsearch.zookeeper.settings;

import com.sonian.elasticsearch.zookeeper.discovery.AbstractZooKeeperTests;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.elasticsearch.common.settings.Settings;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;


/**
 * @author imotov
 */
public class ZooKeeperSettingsManagerTests extends AbstractZooKeeperTests {

    @BeforeClass
    public void createTestPaths() throws Exception {
        startZooKeeper();
    }

    @AfterClass
    public void shutdownZooKeeper() {
        stopZooKeeper();
    }


    @Test public void testSettingsLoading() throws Exception {
        putSettings("# Global\n"
                + "test:\n"
                + "  global.override: globalVersion\n"
                + "  global.data: globalData", true);
        putSettings("# Local\n"
                + "test:\n"
                + "  global.override: clusterVersion\n"
                + "  cluster.data: clusterData", false);

        Settings settings = ZooKeeperSettingsManager.loadZooKeeperSettings(defaultSettings());
        assertThat(settings, notNullValue());
        assertThat(settings.getAsMap().size(), equalTo(3));
        assertThat(settings.get("test.global.data"), equalTo("globalData"));
        assertThat(settings.get("test.global.override"), equalTo("clusterVersion"));
        assertThat(settings.get("test.cluster.data"), equalTo("clusterData"));
    }

    private void putSettings(String settings, boolean global) throws Exception {
        ZooKeeper zooKeeper = zooKeeperFactory().newZooKeeper();
        try {
            String settingsNode;
            if (global) {
                settingsNode = zooKeeperEnvironment().globalSettingsNodePath();
            } else {
                settingsNode = zooKeeperEnvironment().clusterSettingsNodePath();
            }
            int i = 0;
            while (true) {
                i = settingsNode.indexOf("/", i + 1);
                String subPath;
                if (i >= 0) {
                    subPath = settingsNode.substring(0, i);
                } else {
                    subPath = settingsNode;
                }
                if (zooKeeper.exists(subPath, null) == null) {
                    if (i >= 0) {
                        zooKeeper.create(subPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    } else {
                        zooKeeper.create(settingsNode, settings.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        break;
                    }
                } else {
                    if (i < 0) {
                        zooKeeper.setData(settingsNode, settings.getBytes(), -1);
                        break;
                    }
                }
            }

        } finally {
            zooKeeper.close();
        }
    }

}
