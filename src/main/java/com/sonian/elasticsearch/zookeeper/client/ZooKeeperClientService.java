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

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.Bytes;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author imotov
 */
public class ZooKeeperClientService extends AbstractLifecycleComponent<ZooKeeperClient> implements ZooKeeperClient {

    private volatile ZooKeeper zooKeeper;

    private final ZooKeeperEnvironment environment;

    private final ZooKeeperFactory zooKeeperFactory;

    // Make it less than 1M to leave some space for extra zookeeper data
    private static final int MAX_NODE_SIZE = 1000000;

    private static final long CONNECTION_LOSS_RETRY_WAIT = 1000;

    private final int maxNodeSize;

    private final Lock sessionRestartLock = new ReentrantLock();

    private final CopyOnWriteArrayList<SessionStateListener> sessionStateListeners = new CopyOnWriteArrayList<SessionStateListener>();

    @Inject
    public ZooKeeperClientService(Settings settings, ZooKeeperEnvironment environment, ZooKeeperFactory zooKeeperFactory) {
        super(settings);
        this.environment = environment;
        this.zooKeeperFactory = zooKeeperFactory;
        maxNodeSize = settings.getAsInt("zookeeper.maxnodesize", MAX_NODE_SIZE);
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        try {
            final Watcher watcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    switch (event.getState()) {
                        case Expired:
                            resetSession();
                            break;
                        case SyncConnected:
                            notifySessionConnected();
                            break;
                        case Disconnected:
                            notifySessionDisconnected();
                            break;
                    }
                }
            };
            zooKeeper = zooKeeperFactory.newZooKeeper(watcher);
            createPersistentNode(environment.rootNodePath());
            createPersistentNode(environment.clustersNodePath());
        } catch (InterruptedException e) {
            throw new ZooKeeperClientException("Cannot start ZooKeeper client", e);
        }
    }

    @Override
    protected void doStop() throws ElasticSearchException {
        if (zooKeeper != null) {
            try {
                logger.debug("Closing zooKeeper");
                zooKeeper.close();
            } catch (InterruptedException e) {
                // Ignore
            }
            zooKeeper = null;
        }
    }

    @Override
    protected void doClose() throws ElasticSearchException {
    }

    @Override
    public void createPersistentNode(final String path) throws InterruptedException {
        if (!path.startsWith("/")) {
            throw new ZooKeeperClientException("Path " + path + " doesn't start with \"/\"");
        }
        try {
            zooKeeperCall("Cannot create leader node", new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    String[] nodes = path.split("/");
                    String currentPath = "";
                    for (int i = 1; i < nodes.length; i++) {
                        currentPath = currentPath + "/" + nodes[i];
                        if (zooKeeper.exists(currentPath, null) == null) {
                            try {
                                zooKeeper.create(currentPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                                logger.trace("Created node {}", currentPath);
                            } catch (KeeperException.NodeExistsException e) {
                                // Ignore - node was created between our check and attempt to create it
                            }
                        }
                    }
                    return null;
                }
            });
        } catch (KeeperException e) {
            throw new ZooKeeperClientException("Cannot create node at " + path, e);
        }

    }

    @Override
    public void setOrCreatePersistentNode(final String path, final byte[] data) throws InterruptedException {
        if (!lifecycle.started()) {
            throw new ZooKeeperClientException("setOrCreatePersistentNode is called after service was stopped");
        }
        try {
            zooKeeperCall("Cannot create persistent node", new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    if (zooKeeper.exists(path, null) != null) {
                        zooKeeper.setData(path, data, -1);
                    } else {
                        zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                    return null;
                }
            });

        } catch (KeeperException e) {
            throw new ZooKeeperClientException("Cannot persistent node", e);
        }
    }

    @Override
    public void setOrCreateTransientNode(final String path, final byte[] data) throws InterruptedException {
        if (!lifecycle.started()) {
            throw new ZooKeeperClientException("setOrCreateTransientNode is called after service was stopped");
        }
        try {
            try {
                zooKeeperCall("Creating node " + path, new Callable<Object>() {
                    @Override
                    public Object call() throws Exception {
                        zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                        return null;
                    }
                });
            } catch (KeeperException.NodeExistsException e1) {
                // Ignore
            }
        } catch (KeeperException e) {
            throw new ZooKeeperClientException("Cannot create node " + path, e);
        }
    }


    @Override
    public byte[] getOrCreateTransientNode(final String path, final byte[] data, final NodeListener nodeListener) throws InterruptedException {
        if (!lifecycle.started()) {
            throw new ZooKeeperClientException("getOrCreateTransientNode is called after service was stopped");
        }
        while (true) {
            try {
                // First, we try to obtain existing node
                return zooKeeperCall("Getting master data", new Callable<byte[]>() {
                    @Override
                    public byte[] call() throws Exception {
                        return zooKeeper.getData(path, wrapNodeListener(nodeListener), null);
                    }
                });
            } catch (KeeperException.NoNodeException e) {
                try {
                    // If node doesn't exist - we try to create the node and return data without setting the
                    // watcher
                    zooKeeperCall("Cannot create leader node", new Callable<Object>() {
                        @Override
                        public Object call() throws Exception {
                            zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                            return null;
                        }
                    });
                    return data;
                } catch (KeeperException.NodeExistsException e1) {
                    // If node is already created - we will try to read created node on the next iteration of the loop
                } catch (KeeperException e1) {
                    throw new ZooKeeperClientException("Cannot create node " + path, e1);
                }
            } catch (KeeperException e) {
                throw new ZooKeeperClientException("Cannot obtain node" + path, e);
            }
        }
    }

    @Override
    public byte[] getNode(final String path, final NodeListener nodeListener) throws InterruptedException {
        if (!lifecycle.started()) {
            throw new ZooKeeperClientException("getNode is called after service was stopped");
        }

        // If the node doesn't exist, we will use createdWatcher to wait for its appearance
        // If the node exists, we will use deletedWatcher to monitor its disappearance
        final Watcher watcher = wrapNodeListener(nodeListener);
        while (true) {
            try {
                // First we check if the node exists and set createdWatcher
                Stat stat = zooKeeperCall("Checking if node exists", new Callable<Stat>() {
                    @Override
                    public Stat call() throws Exception {
                        return zooKeeper.exists(path, watcher);
                    }
                });

                // If the node exists, returning the current node data
                if (stat != null) {
                    return zooKeeperCall("Getting node data", new Callable<byte[]>() {
                        @Override
                        public byte[] call() throws Exception {
                            return zooKeeper.getData(path, watcher, null);
                        }
                    });
                } else {
                    return null;
                }
            } catch (KeeperException.NoNodeException e) {
                // Node disappeared between exists() and getData() calls
                // We will try again
            } catch (KeeperException e) {
                throw new ZooKeeperClientException("Cannot obtain node " + path, e);
            }
        }
    }

    @Override
    public Set<String> listNodes(final String path, final NodeListChangedListener nodeListChangedListener) throws InterruptedException {
        if (!lifecycle.started()) {
            throw new ZooKeeperClientException("listNodes is called after service was stopped");
        }
        Set<String> res = new HashSet<String>();
        final Watcher watcher = (nodeListChangedListener != null) ?
                new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                            nodeListChangedListener.onNodeListChanged();
                        }
                    }
                } : null;
        try {

            List<String> children = zooKeeperCall("Cannot list nodes", new Callable<List<String>>() {
                @Override
                public List<String> call() throws Exception {
                    return zooKeeper.getChildren(path, watcher);
                }
            });

            if (children == null) {
                return null;
            }
            for (String childPath : children) {
                res.add(extractLastPart(childPath));
            }
            return res;
        } catch (KeeperException e) {
            throw new ZooKeeperClientException("Cannot list nodes", e);
        }
    }

    @Override
    public void deleteNode(final String path) throws InterruptedException {
        if (!lifecycle.started()) {
            throw new ZooKeeperClientException("deleteNode is called after service was stopped");
        }
        try {
            zooKeeperCall("Cannot delete node", new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    zooKeeper.delete(path, -1);
                    return null;
                }
            });
        } catch (KeeperException e) {
            throw new ZooKeeperClientException("Cannot delete node" + path, e);
        }
    }

    @Override
    public void deleteNodeRecursively(final String path) throws InterruptedException {
        if (!lifecycle.started()) {
            throw new ZooKeeperClientException("deleteNode is called after service was stopped");
        }
        try {
            zooKeeperCall("Cannot delete node recursively", new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    deleteNodeRecursively(path);
                    return null;
                }

                private void deleteNodeRecursively(String path) throws InterruptedException, KeeperException {
                    List<String> nodes = zooKeeper.getChildren(path, false);
                    for (String node : nodes) {
                        deleteNodeRecursively(path + "/" + node);
                    }
                    zooKeeper.delete(path, -1);
                }
            });
        } catch (KeeperException e) {
            throw new ZooKeeperClientException("Cannot delete node" + path, e);
        }
    }

    @Override
    public String createLargeSequentialNode(final String pathPrefix, byte[] data) throws InterruptedException {
        if (!lifecycle.started()) {
            throw new ZooKeeperClientException("deleteNode is called after service was stopped");
        }
        String rootPath;
        try {
            final int size = data.length;
            // Create Root node with version and size of the state part
            rootPath = zooKeeperCall("Cannot create node at " + pathPrefix, new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return zooKeeper.create(pathPrefix, Bytes.itoa(size), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
                }
            });
            int chunkNum = 0;
            // Store state part in chunks in case it's too big for a single node
            // It should be able to fit into a single node in most cases
            for (int i = 0; i < size; i += maxNodeSize) {
                final String chunkPath = rootPath + "/" + chunkNum;
                final byte[] chunk;
                if (size > maxNodeSize) {
                    chunk = Arrays.copyOfRange(data, i, Math.min(size, i + maxNodeSize));
                } else {
                    chunk = data;
                }
                zooKeeperCall("Cannot create node at " + chunkPath, new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        return zooKeeper.create(chunkPath, chunk, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                });
                chunkNum++;
            }
        } catch (KeeperException e) {
            throw new ZooKeeperClientException("Cannot create node at " + pathPrefix, e);
        }
        return rootPath;
    }

    @Override
    public void deleteLargeNode(final String path) throws InterruptedException {
        if (!lifecycle.started()) {
            throw new ZooKeeperClientException("deleteNode is called after service was stopped");
        }
        try {
            zooKeeperCall("Cannot delete node at " + path, new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    List<String> children = zooKeeper.getChildren(path, null);
                    for (String child : children) {
                        zooKeeper.delete(path + "/" + child, -1);
                    }
                    zooKeeper.delete(path, -1);
                    return null;
                }
            });
        } catch (KeeperException e) {
            throw new ZooKeeperClientException("Cannot delete node at " + path, e);
        }
    }

    @Override
    public byte[] getLargeNode(final String path) throws InterruptedException {
        if (!lifecycle.started()) {
            throw new ZooKeeperClientException("getLargeNode is called after service was stopped");
        }
        try {
            byte[] sizeBuf = zooKeeperCall("Cannot read node at " + path, new Callable<byte[]>() {
                @Override
                public byte[] call() throws Exception {
                    return zooKeeper.getData(path, null, null);
                }
            });
            final int size = Bytes.atoi(sizeBuf);
            int chunkNum = 0;

            BytesStreamOutput buf = new BytesStreamOutput(size);
            int offset = 0;
            while (offset < size) {
                final String chunkPath = path + "/" + chunkNum;
                byte[] chunk = zooKeeperCall("Cannot read node", new Callable<byte[]>() {
                    @Override
                    public byte[] call() throws Exception {
                        return zooKeeper.getData(chunkPath, null, null);
                    }
                });
                if (chunk == null || chunk.length == 0) {
                    return null;
                }
                buf.write(chunk);
                offset += chunk.length;
                chunkNum++;
            }
            return buf.copiedByteArray();
        } catch (KeeperException.NoNodeException e) {
            // This means that a new version of state is already posted and this version is
            // getting deleted - exit
            return null;
        } catch (KeeperException e) {
            throw new ZooKeeperClientException("Cannot read node at " + path, e);
        } catch (IOException e) {
            throw new ZooKeeperClientException("Cannot read node at " + path, e);
        }
    }

    @Override
    public void addSessionStateListener(SessionStateListener sessionStateListener) {
        sessionStateListeners.add(sessionStateListener);
    }

    @Override
    public void removeSessionStateListener(SessionStateListener sessionStateListener) {
        sessionStateListeners.remove(sessionStateListener);
    }

    @Override
    public boolean verifyConnection(TimeValue timeout) throws InterruptedException {
        if (connected()) {
            final AtomicBoolean stats = new AtomicBoolean(false);
            final CountDownLatch latch = new CountDownLatch(1);
            zooKeeper.exists("/", null, new AsyncCallback.StatCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, Stat stat) {
                    stats.set(stat != null);
                    latch.countDown();
                }
            }, null);
            latch.await(timeout.getMillis(), TimeUnit.MILLISECONDS);
            return stats.get();
        } else {
            return false;
        }
    }

    @Override
    public boolean connected() {
        return zooKeeper != null && zooKeeper.getState() == ZooKeeper.States.CONNECTED;
    }

    @Override
    public long sessionId() {
        if (!lifecycle.started()) {
            throw new ZooKeeperClientException("sessionId is called after service was stopped");
        }
        return zooKeeper.getSessionId();
    }

    private void resetSession() {
        if (lifecycle.started()) {
            zooKeeper.sync("/", new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(int i, String s, Object o) {
                    sessionRestartLock.lock();
                    try {
                        logger.trace("Checking if ZooKeeper session should be restarted");
                        if (lifecycle.started()) {
                            if (!connected()) {
                                logger.info("Restarting ZooKeeper discovery");
                                try {
                                    logger.trace("Stopping ZooKeeper");
                                    doStop();
                                } catch (Exception ex) {
                                    logger.error("Error stopping ZooKeeper", ex);
                                }
                                while (lifecycle.started()) {
                                    try {
                                        logger.trace("Starting ZooKeeper");
                                        doStart();
                                        logger.trace("Started ZooKeeper");
                                        notifySessionReset();
                                        return;
                                    } catch (ZooKeeperClientException ex) {
                                        if (ex.getCause() != null && ex.getCause() instanceof InterruptedException) {
                                            logger.info("ZooKeeper startup was interrupted", ex);
                                            Thread.currentThread().interrupt();
                                            return;
                                        }
                                        logger.warn("Error starting ZooKeeper ", ex);
                                    }
                                    try {
                                        Thread.sleep(1000);
                                    } catch (InterruptedException ex) {
                                        return;
                                    }
                                }
                            } else {
                                logger.trace("ZooKeeper is already restarted. Ignoring");
                            }
                        }
                    } finally {
                        sessionRestartLock.unlock();
                    }
                }
            }
                    , null);
        }

    }

    private void notifySessionReset() {
        for (SessionStateListener listener : sessionStateListeners) {
            listener.sessionExpired();
        }
    }

    private void notifySessionConnected() {
        for (SessionStateListener listener : sessionStateListeners) {
            listener.sessionConnected();
        }
    }

    private void notifySessionDisconnected() {
        for (SessionStateListener listener : sessionStateListeners) {
            listener.sessionDisconnected();
        }
    }

    private <T> T zooKeeperCall(String reason, Callable<T> callable) throws InterruptedException, KeeperException {
        boolean connectionLossReported = false;
        while (true) {
            try {
                if (zooKeeper == null) {
                    throw new ZooKeeperClientException("ZooKeeper is not available - reconnecting");
                }
                return callable.call();
            } catch (KeeperException.ConnectionLossException ex) {
                if (!connectionLossReported) {
                    logger.debug("Connection Loss Exception");
                    connectionLossReported = true;
                }
                Thread.sleep(CONNECTION_LOSS_RETRY_WAIT);
            } catch (KeeperException.SessionExpiredException e) {
                logger.warn("Session Expired Exception");
                resetSession();
                throw new ZooKeeperClientSessionExpiredException(reason, e);
            } catch (KeeperException e) {
                throw e;
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                logger.warn("Unknown Exception", e);
                throw new ZooKeeperClientException(reason, e);
            }
        }
    }


    private Watcher wrapNodeListener(final NodeListener nodeListener) {
        if (nodeListener != null) {
            return new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    switch (event.getType()) {
                        case NodeCreated:
                            nodeListener.onNodeCreated(event.getPath());
                            break;
                        case NodeDeleted:
                            nodeListener.onNodeDeleted(event.getPath());
                            break;
                        case NodeDataChanged:
                            nodeListener.onNodeDataChanged(event.getPath());
                            break;
                    }
                }
            };
        } else {
            return null;
        }
    }

    private String extractLastPart(String path) {
        int index = path.lastIndexOf('/');
        if (index >= 0) {
            return path.substring(index + 1);
        } else {
            return path;
        }
    }


}
