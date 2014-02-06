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

import com.sonian.elasticsearch.zookeeper.client.ZooKeeperClientService;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.Classes;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.logging.log4j.LogConfigurator;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.loader.SettingsLoader;
import org.elasticsearch.common.settings.loader.SettingsLoaderFactory;
import com.sonian.elasticsearch.zookeeper.client.ZooKeeperEnvironment;
import com.sonian.elasticsearch.zookeeper.client.ZooKeeperFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPreparer;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;

/**
 * @author imotov
 */
public final class ZooKeeperSettingsManager extends AbstractLifecycleComponent<ZooKeeperSettingsManager> {

    private final ZooKeeperFactory factory;

    private final ZooKeeperEnvironment environment;

    private final ZooKeeperClientService zooKeeperClientService;

    private ZooKeeperSettingsManager(Settings settings) {
        this(settings, ClusterName.clusterNameFromSettings(settings));
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        zooKeeperClientService.start();
    }

    @Override
    protected void doStop() throws ElasticSearchException {
        zooKeeperClientService.stop();
    }

    @Override
    protected void doClose() throws ElasticSearchException {
        zooKeeperClientService.close();
    }

    private ZooKeeperSettingsManager(Settings settings, ClusterName clusterName) {
        super(settings);
        environment = new ZooKeeperEnvironment(settings, clusterName);
        factory = new ZooKeeperFactory(settings);
        zooKeeperClientService = new ZooKeeperClientService(settings, environment, factory);
    }

    public static Settings loadZooKeeperSettings(Settings settings) {
        ZooKeeperSettingsManager settingsManager = new ZooKeeperSettingsManager(settings);
        try {
            settingsManager.start();
            return ImmutableSettings.settingsBuilder()
                    .put(settingsManager.loadGlobalSettings())
                    .put(settingsManager.loadClusterSettings())
                    .build();
        } catch (InterruptedException e) {
            // Ignore
        } finally {
            settingsManager.close();
        }
        return ImmutableSettings.Builder.EMPTY_SETTINGS;
    }

    private Map<String, String> loadGlobalSettings() throws InterruptedException {
        return loadSettings(environment.globalSettingsNodePath());
    }

    private Map<String, String> loadClusterSettings() throws InterruptedException {
        return loadSettings(environment.clusterSettingsNodePath());
    }

    private Map<String, String> loadSettings(String path) throws InterruptedException {
        byte[] settingsBytes = zooKeeperClientService.getNode(path, null);
        if (settingsBytes != null) {
            SettingsLoader loader = SettingsLoaderFactory.loaderFromSource(new String(settingsBytes));
            try {
                return loader.load(settingsBytes);
            } catch (IOException ex) {
                throw new ElasticSearchException("Cannot load settings ", ex);
            }
        } else {
            return Collections.emptyMap();
        }
    }

    private void storeGlobalSettingsBytes(byte[] settingsBytes) throws InterruptedException {
        zooKeeperClientService.createPersistentNode(environment.clusterNodePath());
        zooKeeperClientService.setOrCreatePersistentNode(environment.globalSettingsNodePath(), settingsBytes);
    }

    private void storeClusterSettingsBytes(byte[] settingsBytes) throws InterruptedException {
        zooKeeperClientService.createPersistentNode(environment.clusterNodePath());
        zooKeeperClientService.setOrCreatePersistentNode(environment.clusterSettingsNodePath(), settingsBytes);
    }

    private byte[] loadGlobalSettingsBytes() throws InterruptedException {
        return zooKeeperClientService.getNode(environment.globalSettingsNodePath(), null);
    }

    private byte[] loadClusterSettingsBytes() throws InterruptedException {
        return zooKeeperClientService.getNode(environment.clusterSettingsNodePath(), null);
    }

    private static byte[] loadSettings(Settings settings, boolean global) {
        ZooKeeperSettingsManager settingsManager = new ZooKeeperSettingsManager(settings);
        try {
            settingsManager.start();
            if (global) {
                return settingsManager.loadGlobalSettingsBytes();
            } else {
                return settingsManager.loadClusterSettingsBytes();
            }
        } catch (InterruptedException e) {
            // Ignore
        } finally {
            settingsManager.close();
        }
        return null;
    }

    private static void storeSettings(Settings settings, byte[] settingsBytes, boolean global) {
        ZooKeeperSettingsManager settingsManager = new ZooKeeperSettingsManager(settings);
        try {
            settingsManager.start();
            if (global) {
                settingsManager.storeGlobalSettingsBytes(settingsBytes);
            } else {
                settingsManager.storeClusterSettingsBytes(settingsBytes);
            }
        } catch (InterruptedException e) {
            // Ignore
        } finally {
            settingsManager.close();
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage:");
            System.out.println("    set <settings file>");
            System.out.println("    set-global <settings file>");
            System.out.println("    get");
            System.out.println("    get-global");
            return;
        }

        Tuple<Settings, Environment> initialSettings = InternalSettingsPreparer.prepareSettings(EMPTY_SETTINGS, true);

        Settings settings = InternalSettingsPreparer.prepareSettings(initialSettings.v1(), true).v1();

        setupLogging(settings);
        Loggers.disableConsoleLogging();

        if (args[0].equals("set") || args[0].equals("set-global")) {
            if (args.length < 2) {
                System.out.println("Properties file has to be specified");
            } else {
                byte[] settingsBytes = loadSettingsFile(args[1]);
                storeSettings(settings, settingsBytes, args[0].equals("set-global"));
            }
        } else if (args[0].equals("get") || args[0].equals("get-global")) {
            byte[] loadedSettings = loadSettings(settings, args[0].equals("get-global"));
            if (loadedSettings != null) {
                try{
                System.out.write(loadedSettings);
                }catch (IOException ex) {
                    // Ignore
                }
            }
        } else {
            System.out.println("Unknown command " + args[0]);
        }
    }

    private static void setupLogging(Settings settings) {
        try {
            Classes.getDefaultClassLoader().loadClass("org.apache.log4j.Logger");
            LogConfigurator.configure(settings);
        } catch (ClassNotFoundException e) {
            // no log4j
        } catch (NoClassDefFoundError e) {
            // no log4j
        } catch (Exception e) {
            System.err.println("Failed to configure logging...");
            e.printStackTrace();
        }
    }

    private static byte[] loadSettingsFile(String path) {
        File file = new File(path);
        try {
            return Streams.copyToByteArray(file);
        } catch (IOException ex) {
            throw new ElasticSearchException("Cannot load settings file " + path, ex);
        }
    }

}
