/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A class encapsulating some of the logic around metadata.
 * <p>
 * This class is shared by the client thread (for partitioning) and the background sender thread.
 *
 * Metadata is maintained for only a subset of topics, which can be added to over time. When we request metadata for a
 * topic we don't have any metadata for it will trigger a metadata update.
 * <p>
 * If topic expiry is enabled for the metadata, any topic that has not been used within the expiry interval
 * is removed from the metadata refresh set after an update. Consumers disable topic expiry since they explicitly
 * manage topics while producers rely on topic expiry to limit the refresh set.
 */
public final class Metadata implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(Metadata.class);

    public static final long TOPIC_EXPIRY_MS = 5 * 60 * 1000;
    private static final long TOPIC_EXPIRY_NEEDS_UPDATE = -1L;

    private final long refreshBackoffMs;
    private final long metadataExpireMs;
    private int version;
    private long lastRefreshMs;
    private long lastSuccessfulRefreshMs;
    private AuthenticationException authenticationException;
    private Cluster cluster;
    private boolean needUpdate;
    /* Topics with expiry time */
    private final Map<String, Long> topics;
    private final List<Listener> listeners;
    private final ClusterResourceListeners clusterResourceListeners;
    private boolean needMetadataForAllTopics;
    private final boolean allowAutoTopicCreation;
    private final boolean topicExpiryEnabled;
    private boolean isClosed;
    private final long topicExpiryMs;

    private final long maxClusterMetadataExpireTimeMs;
    private int nodesTriedSinceLastSuccessfulRefresh;
    private boolean forceClusterMetadataUpdateFromBootstrap;

    public Metadata(long refreshBackoffMs, long metadataExpireMs, boolean allowAutoTopicCreation) {
        this(refreshBackoffMs, metadataExpireMs, allowAutoTopicCreation, false, new ClusterResourceListeners());
    }

    public Metadata(long refreshBackoffMs, long metadataExpireMs, boolean allowAutoTopicCreation,
                    boolean topicExpiryEnabled, ClusterResourceListeners clusterResourceListeners) {
        this(refreshBackoffMs, metadataExpireMs, allowAutoTopicCreation, topicExpiryEnabled, clusterResourceListeners, TOPIC_EXPIRY_MS,
            Long.MAX_VALUE);
    }

    /**
     * Create a new Metadata instance
     * @param refreshBackoffMs The minimum amount of time that must expire between metadata refreshes to avoid busy
     *        polling
     * @param metadataExpireMs The maximum amount of time that metadata can be retained without refresh
     * @param allowAutoTopicCreation If this and the broker config 'auto.create.topics.enable' are true, topics that
     *                               don't exist will be created by the broker when a metadata request is sent
     * @param topicExpiryEnabled If true, enable expiry of unused topics
     * @param clusterResourceListeners List of ClusterResourceListeners which will receive metadata updates.
     */
    public Metadata(long refreshBackoffMs, long metadataExpireMs, boolean allowAutoTopicCreation,
                    boolean topicExpiryEnabled, ClusterResourceListeners clusterResourceListeners, long topicExpiryMs,
                    long metadataClusterMetadataExpireTimeMs) {
        this.refreshBackoffMs = refreshBackoffMs;
        this.metadataExpireMs = metadataExpireMs;
        this.allowAutoTopicCreation = allowAutoTopicCreation;
        this.topicExpiryEnabled = topicExpiryEnabled;
        this.lastRefreshMs = 0L;
        this.lastSuccessfulRefreshMs = 0L;
        this.version = 0;
        this.cluster = Cluster.empty();
        this.needUpdate = false;
        this.topics = new HashMap<>();
        this.listeners = new ArrayList<>();
        this.clusterResourceListeners = clusterResourceListeners;
        this.needMetadataForAllTopics = false;
        this.isClosed = false;
        this.topicExpiryMs = topicExpiryMs;
        this.maxClusterMetadataExpireTimeMs = metadataClusterMetadataExpireTimeMs;
        this.nodesTriedSinceLastSuccessfulRefresh = 0;
        this.forceClusterMetadataUpdateFromBootstrap = false;
    }

    /**
     * Get the current cluster info without blocking
     */
    public synchronized Cluster fetch() {
        return this.cluster;
    }

    /**
     * Add the topic to maintain in the metadata. If topic expiry is enabled, expiry time
     * will be reset on the next update.
     */
    public synchronized void add(String topic) {
        Objects.requireNonNull(topic, "topic cannot be null");
        if (topics.put(topic, TOPIC_EXPIRY_NEEDS_UPDATE) == null) {
            requestUpdateForNewTopics();
        }
    }

    /**
     * Increment the nodesTriedSinceLastSuccessfulRefresh
     */
    public synchronized void incrementNodesTriedSinceLastSuccessfulRefresh() {
        this.nodesTriedSinceLastSuccessfulRefresh++;
    }

    /**
     * Whether the client should update the cluster metadata by resolving the bootstrap server again
     * @param nowMs
     * @return true if client hasn't refreshed cluster metadata for maxClusterMetadataExpireTimeMs and
     * has tried connecting to at least one node in current node set; or forceClusterMetadataUpdateFromBootstrap
     * has been set by receiving stale metadata from a different cluster
     */
    public synchronized boolean shouldUpdateClusterMetadataFromBootstrap(long nowMs) {
        return (this.nodesTriedSinceLastSuccessfulRefresh >= 1 &&
            this.lastSuccessfulRefreshMs + this.maxClusterMetadataExpireTimeMs <= nowMs) ||
            this.forceClusterMetadataUpdateFromBootstrap;
    }

    /**
     * Return the next time when the current cluster info can be updated (i.e., backoff time has elapsed).
     *
     * @param nowMs current time in ms
     * @return remaining time in ms till the cluster info can be updated again
     */
    public synchronized long timeToAllowUpdate(long nowMs) {
        return Math.max(this.lastRefreshMs + this.refreshBackoffMs - nowMs, 0);
    }

    /**
     * The next time to update the cluster info is the maximum of the time the current info will expire and the time the
     * current info can be updated (i.e. backoff time has elapsed); If an update has been request then the expiry time
     * is now
     *
     * @param nowMs current time in ms
     * @return remaining time in ms till updating the cluster info
     */
    public synchronized long timeToNextUpdate(long nowMs) {
        long timeToExpire = needUpdate ? 0 : Math.max(this.lastSuccessfulRefreshMs + this.metadataExpireMs - nowMs, 0);
        return Math.max(timeToExpire, timeToAllowUpdate(nowMs));
    }

    /**
     * Request an update of the current cluster metadata info, return the current version before the update
     */
    public synchronized int requestUpdate() {
        this.needUpdate = true;
        return this.version;
    }

    /**
     * Request an update of the current cluster metadata info by resolving the bootstrap server and randomly pick
     * a node from the resolved node set. This happens when client receives stale metadata response from brokers in
     * a different cluster and need to refresh the cluster metadata without waiting for maxClusterMetadataExpireTimeMs
     */
    public synchronized void requestClusterMetadataUpdateFromBootstrap() {
        this.forceClusterMetadataUpdateFromBootstrap = true;
    }

    /**
     * Check whether an update has been explicitly requested.
     * @return true if an update was requested, false otherwise
     */
    public synchronized boolean updateRequested() {
        return this.needUpdate;
    }

    /**
     * If any non-retriable authentication exceptions were encountered during
     * metadata update, clear and return the exception.
     */
    public synchronized AuthenticationException getAndClearAuthenticationException() {
        if (authenticationException != null) {
            AuthenticationException exception = authenticationException;
            authenticationException = null;
            return exception;
        } else
            return null;
    }

    /**
     * Wait for metadata update until the current version is larger than the last version we know of
     */
    public synchronized void awaitUpdate(final int lastVersion, final long maxWaitMs) throws InterruptedException {
        if (maxWaitMs < 0)
            throw new IllegalArgumentException("Max time to wait for metadata updates should not be < 0 milliseconds");

        long begin = System.currentTimeMillis();
        long remainingWaitMs = maxWaitMs;
        while ((this.version <= lastVersion) && !isClosed()) {
            AuthenticationException ex = getAndClearAuthenticationException();
            if (ex != null)
                throw ex;
            if (remainingWaitMs != 0)
                wait(remainingWaitMs);
            long elapsed = System.currentTimeMillis() - begin;
            if (elapsed >= maxWaitMs)
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            remainingWaitMs = maxWaitMs - elapsed;
        }
        if (isClosed())
            throw new KafkaException("Requested metadata update after close");
    }

    /**
     * Replace the current set of topics maintained to the one provided.
     * If topic expiry is enabled, expiry time of the topics will be
     * reset on the next update.
     * @param topics
     */
    public synchronized void setTopics(Collection<String> topics) {
        if (!this.topics.keySet().containsAll(topics)) {
            requestUpdateForNewTopics();
        }
        this.topics.clear();
        for (String topic : topics)
            this.topics.put(topic, TOPIC_EXPIRY_NEEDS_UPDATE);
    }

    /**
     * Get the list of topics we are currently maintaining metadata for
     */
    public synchronized Set<String> topics() {
        return new HashSet<>(this.topics.keySet());
    }

    /**
     * Check if a topic is already in the topic set.
     * @param topic topic to check
     * @return true if the topic exists, false otherwise
     */
    public synchronized boolean containsTopic(String topic) {
        return this.topics.containsKey(topic);
    }

    /**
     * Updates the cluster metadata. If topic expiry is enabled, expiry time
     * is set for topics if required and expired topics are removed from the metadata.
     *
     * @param newCluster the cluster containing metadata for topics with valid metadata
     * @param unavailableTopics topics which are non-existent or have one or more partitions whose
     *        leader is not known
     * @param now current time in milliseconds
     */
    public synchronized void update(Cluster newCluster, Set<String> unavailableTopics, long now) {
        Objects.requireNonNull(newCluster, "cluster should not be null");
        if (isClosed())
            throw new IllegalStateException("Update requested after metadata close");

        if (!validateCluster(newCluster)) {
            //if validateCluster fails, do not update metadataCache with the wrong cluster information,
            //just return and wait for next update
            //
            //here we don't blacklist this node from the cluster's
            //node set since we don't have enough information from the response to map to the actual node,
            //and since there are usually hours to days interval before we put a removed broker to a different
            //cluster, clients should either find another node in cached node set or resolved bootstrap server
            //again and find a new node to send update metadata request, it should be ok to not blacklist this node
            requestClusterMetadataUpdateFromBootstrap();
            return;
        }

        this.needUpdate = false;
        this.lastRefreshMs = now;
        this.lastSuccessfulRefreshMs = now;
        this.version += 1;

        if (topicExpiryEnabled) {
            // Handle expiry of topics from the metadata refresh set.
            for (Iterator<Map.Entry<String, Long>> it = topics.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, Long> entry = it.next();
                long expireMs = entry.getValue();
                if (expireMs == TOPIC_EXPIRY_NEEDS_UPDATE) {
                    entry.setValue(Long.MAX_VALUE - now > topicExpiryMs ? now + topicExpiryMs : Long.MAX_VALUE);
                } else if (expireMs <= now) {
                    it.remove();
                    log.debug("Removing unused topic {} from the metadata list, expiryMs {} now {}", entry.getKey(), expireMs, now);
                }
            }
        }

        for (Listener listener: listeners)
            listener.onMetadataUpdate(newCluster, unavailableTopics);

        if (this.needMetadataForAllTopics) {
            // the listener may change the interested topics, which could cause another metadata refresh.
            // If we have already fetched all topics, however, another fetch should be unnecessary.
            this.needUpdate = false;
            this.cluster = getClusterForCurrentTopics(newCluster);
        } else {
            this.cluster = newCluster;
        }

        // The bootstrap cluster is guaranteed not to have any useful information
        if (!newCluster.isBootstrapConfigured()) {
            String previousClusterId = this.cluster.clusterResource().clusterId();
            String newClusterId = newCluster.clusterResource().clusterId();
            if (newClusterId == null ? previousClusterId != null : !newClusterId.equals(previousClusterId))
                log.info("Cluster ID: {}", newClusterId);
            clusterResourceListeners.onUpdate(newCluster.clusterResource());
        }

        notifyAll();
        log.debug("Updated cluster metadata version {} to {}", this.version, this.cluster);
    }

    /**
     * Record an attempt to update the metadata that failed. We need to keep track of this
     * to avoid retrying immediately.
     */
    public synchronized void failedUpdate(long now, AuthenticationException authenticationException) {
        this.lastRefreshMs = now;
        this.authenticationException = authenticationException;
        if (authenticationException != null)
            this.notifyAll();
    }

    /**
     * @return The current metadata version
     */
    public synchronized int version() {
        return this.version;
    }

    /**
     * The last time metadata was successfully updated.
     */
    public synchronized long lastSuccessfulUpdate() {
        return this.lastSuccessfulRefreshMs;
    }

    public boolean allowAutoTopicCreation() {
        return allowAutoTopicCreation;
    }

    /**
     * Set state to indicate if metadata for all topics in Kafka cluster is required or not.
     * @param needMetadataForAllTopics boolean indicating need for metadata of all topics in cluster.
     */
    public synchronized void needMetadataForAllTopics(boolean needMetadataForAllTopics) {
        if (needMetadataForAllTopics && !this.needMetadataForAllTopics) {
            requestUpdateForNewTopics();
        }
        this.needMetadataForAllTopics = needMetadataForAllTopics;
    }

    /**
     * Get whether metadata for all topics is needed or not
     */
    public synchronized boolean needMetadataForAllTopics() {
        return this.needMetadataForAllTopics;
    }

    /**
     * Add a Metadata listener that gets notified of metadata updates
     */
    public synchronized void addListener(Listener listener) {
        this.listeners.add(listener);
    }

    /**
     * Stop notifying the listener of metadata updates
     */
    public synchronized void removeListener(Listener listener) {
        this.listeners.remove(listener);
    }

    /**
     * "Close" this metadata instance to indicate that metadata updates are no longer possible. This is typically used
     * when the thread responsible for performing metadata updates is exiting and needs a way to relay this information
     * to any other thread(s) that could potentially wait on metadata update to come through.
     */
    @Override
    public synchronized void close() {
        this.isClosed = true;
        this.notifyAll();
    }

    /**
     * Check if this metadata instance has been closed. See {@link #close()} for more information.
     * @return True if this instance has been closed; false otherwise
     */
    public synchronized boolean isClosed() {
        return this.isClosed;
    }

    /**
     * MetadataUpdate Listener
     */
    public interface Listener {
        /**
         * Callback invoked on metadata update.
         *
         * @param cluster the cluster containing metadata for topics with valid metadata
         * @param unavailableTopics topics which are non-existent or have one or more partitions whose
         *        leader is not known
         */
        void onMetadataUpdate(Cluster cluster, Set<String> unavailableTopics);
    }

    private synchronized void requestUpdateForNewTopics() {
        // Override the timestamp of last refresh to let immediate update.
        this.lastRefreshMs = 0;
        requestUpdate();
    }

    private boolean validateCluster(Cluster newCluster) {
        String previousClusterId = this.cluster.clusterResource().clusterId();
        String newClusterId = newCluster.clusterResource().clusterId();
        boolean validateResult = true;

        if (previousClusterId != null && newClusterId != null && !previousClusterId.equals(newClusterId)) {
            // kafka cluster id is unique.
            // On client side, the cluster id in Metadata is only null during bootstrap, and client is
            // expected to talk to the same cluster during its life cycle. Therefore if cluster id changes
            // during metadata update, meaning this metadata update response is from a different cluster,
            // client should reject this response and not update cached cluster to the wrong cluster

            // Since removing brokers and adding to another cluster can be common operation,
            // it might be more suitable to just throw an exception for update and fail this update operation
            // instead of bringing down the client completely, so that the metadata can be updated later from
            // other brokers in the same cluster.

            // this code path will only get executed when all brokers in original cluster has been removed and
            // one/some brokers have been added to another. If only some of the original brokers were removed/added
            // to another cluster, the client should get updated metadata with valid brokers from other hosts.
            // so we can just throw an exception and close the network client

            log.error("Received metadata from a different cluster {}, current cluster {} has no valid brokers anymore,"
                + "please reboot the producer/consumer", newClusterId, previousClusterId);

            validateResult = false;

        }

        return validateResult;
    }

    private Cluster getClusterForCurrentTopics(Cluster cluster) {
        Set<String> unauthorizedTopics = new HashSet<>();
        Set<String> invalidTopics = new HashSet<>();
        Collection<PartitionInfo> partitionInfos = new ArrayList<>();
        List<Node> nodes = Collections.emptyList();
        Set<String> internalTopics = Collections.emptySet();
        Node controller = null;
        String clusterId = null;
        if (cluster != null) {
            clusterId = cluster.clusterResource().clusterId();
            internalTopics = cluster.internalTopics();
            unauthorizedTopics.addAll(cluster.unauthorizedTopics());
            unauthorizedTopics.retainAll(this.topics.keySet());

            invalidTopics.addAll(cluster.invalidTopics());
            invalidTopics.addAll(this.cluster.invalidTopics());

            for (String topic : this.topics.keySet()) {
                List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);
                if (!partitionInfoList.isEmpty()) {
                    partitionInfos.addAll(partitionInfoList);
                }
            }
            nodes = cluster.nodes();
            controller  = cluster.controller();
        }
        return new Cluster(clusterId, nodes, partitionInfos, unauthorizedTopics, invalidTopics, internalTopics, controller);
    }
}
