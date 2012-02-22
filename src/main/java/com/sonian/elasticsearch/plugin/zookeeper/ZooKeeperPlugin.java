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


package com.sonian.elasticsearch.plugin.zookeeper;

import com.sonian.elasticsearch.rest.zookeeper.RestZooKeeperStatusAction;
import com.sonian.elasticsearch.zookeeper.settings.ZooKeeperSettingsManager;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

/**
 * @author imotov
 */
public class ZooKeeperPlugin extends AbstractPlugin {

    private final Settings settings;

    public ZooKeeperPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override public String name() {
        return "zookeeper";
    }

    @Override public String description() {
        return "ZooKeeper Plugin Version: " + Version.number() + " (" + Version.date() + ")";
    }

    @Override public Settings additionalSettings() {
        if (settings.getAsBoolean("sonian.elasticsearch.zookeeper.settings.enabled", false)) {
            return ZooKeeperSettingsManager.loadZooKeeperSettings(settings);
        } else {
            return super.additionalSettings();
        }
    }

    @Override
    public void processModule(Module module) {
        if (module instanceof RestModule) {
            ((RestModule) module).addRestAction(RestZooKeeperStatusAction.class);
        }
    }

    public void onModule(RestModule restModule) {
        restModule.addRestAction(RestZooKeeperStatusAction.class);
    }
}
