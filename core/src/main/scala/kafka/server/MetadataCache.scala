/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.api._
import kafka.cluster.{Broker, EndPoint}
import kafka.controller.StateChangeLogger
import kafka.utils.CoreUtils._
import kafka.utils.Logging
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{MetadataResponse, UpdateMetadataRequest}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.{Cluster, Node, PartitionInfo, TopicPartition}

import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.{Collections, Optional}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.{Seq, Set, mutable}


/**
 *  A cache for the state (e.g., current leader) of each partition. This cache is updated through
 *  UpdateMetadataRequest from the controller. Every broker maintains the same cache, asynchronously.
 */
class MetadataCache(brokerId: Int, localClusterId: String = "defaultClusterId", federationEnabled: Boolean = false) extends Logging {

  private val partitionMetadataLock = new ReentrantReadWriteLock()
  //this is the cache state. every MetadataSnapshot instance is immutable, and updates (performed under a lock)
  //replace the value with a completely new one. this means reads (which are not under any lock) need to grab
  //the value of this var (into a val) ONCE and retain that read copy for the duration of their operation.
  //multiple reads of this value risk getting different snapshots.
  @volatile private var metadataSnapshot: MetadataSnapshot = if (federationEnabled)
    MultiClusterMetadataSnapshot(partitionStatesMap = mutable.AnyRefMap.empty, controllerIdOpt = None,
      multiClusterAliveBrokers = mutable.Map.empty, multiClusterAliveNodes = mutable.Map.empty)
    else
    SingleClusterMetadataSnapshot(partitionStatesMap = mutable.AnyRefMap.empty, controllerIdOpt = None,
      aliveBrokersMap = mutable.LongMap.empty, aliveNodesMap = mutable.LongMap.empty)

  this.logIdent = s"[MetadataCache brokerId=$brokerId] "
  private val stateChangeLogger = new StateChangeLogger(brokerId, inControllerContext = false, None)

  // This method is the main hotspot when it comes to the performance of metadata requests,
  // we should be careful about adding additional logic here. Relatedly, `brokers` is
  // `Iterable[Integer]` instead of `Iterable[Int]` to avoid a collection copy.
  // filterUnavailableEndpoints exists to support v0 MetadataResponses
  private def getEndpoints(snapshot: MetadataSnapshot, brokers: Iterable[java.lang.Integer], listenerName: ListenerName, filterUnavailableEndpoints: Boolean): Seq[Node] = {
    val result = new mutable.ArrayBuffer[Node](math.min(snapshot.aliveBrokers.size, brokers.size))
    brokers.foreach { brokerId =>
      val endpoint = getAliveEndpoint(snapshot, brokerId, listenerName) match {
        case None => if (!filterUnavailableEndpoints) Some(new Node(brokerId, "", -1)) else None
        case Some(node) => Some(node)
      }
      endpoint.foreach(result +=)
    }
    result
  }

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  // If errorUnavailableListeners=true, return LISTENER_NOT_FOUND if listener is missing on the broker.
  // Otherwise, return LEADER_NOT_AVAILABLE for broker unavailable and missing listener (Metadata response v5 and below).
  private def getPartitionMetadata(snapshot: MetadataSnapshot, topic: String, listenerName: ListenerName, errorUnavailableEndpoints: Boolean,
                                   errorUnavailableListeners: Boolean): Option[Iterable[MetadataResponse.PartitionMetadata]] = {
    val result = new ArrayBuffer[MetadataResponse.PartitionMetadata]
    for (partitionsMap <- snapshot.partitionStates.get(topic)) {
      for (partitionEntry <- partitionsMap) {
        val partitionId = partitionEntry._1.toInt
        val partitionState = partitionEntry._2
        val topicPartition = new TopicPartition(topic, partitionId)
        val leaderBrokerId = partitionState.leader
        val leaderEpoch = partitionState.leaderEpoch
        val maybeLeader = getAliveEndpoint(snapshot, leaderBrokerId, listenerName)
        val replicas = partitionState.replicas.asScala
        val replicaInfo = getEndpoints(snapshot, replicas, listenerName, errorUnavailableEndpoints)
        val offlineReplicaInfo = getEndpoints(snapshot, partitionState.offlineReplicas.asScala, listenerName, errorUnavailableEndpoints)

        val isr = partitionState.isr.asScala
        val isrInfo = getEndpoints(snapshot, isr, listenerName, errorUnavailableEndpoints)
        maybeLeader match {
          case None =>
            val error = if (!snapshot.aliveBrokers.contains(brokerId)) { // we are already holding the read lock
              debug(s"Error while fetching metadata for $topicPartition: leader not available")
              Errors.LEADER_NOT_AVAILABLE
            } else {
              debug(s"Error while fetching metadata for $topicPartition: listener $listenerName not found on leader $leaderBrokerId")
              if (errorUnavailableListeners) Errors.LISTENER_NOT_FOUND else Errors.LEADER_NOT_AVAILABLE
            }
            result += new MetadataResponse.PartitionMetadata(error, partitionId, Node.noNode(),
              Optional.empty(), replicaInfo.asJava, isrInfo.asJava,
              offlineReplicaInfo.asJava)

          case Some(leader) =>
            if (replicaInfo.size < replicas.size) {
              debug(s"Error while fetching metadata for $topicPartition: replica information not available for " +
                s"following brokers ${replicas.filterNot(replicaInfo.map(_.id).contains).mkString(",")}")

              result += new MetadataResponse.PartitionMetadata(Errors.REPLICA_NOT_AVAILABLE, partitionId, leader,
                Optional.empty(), replicaInfo.asJava, isrInfo.asJava, offlineReplicaInfo.asJava)
            } else if (isrInfo.size < isr.size) {
              debug(s"Error while fetching metadata for $topicPartition: in sync replica information not available for " +
                s"following brokers ${isr.filterNot(isrInfo.map(_.id).contains).mkString(",")}")
              result += new MetadataResponse.PartitionMetadata(Errors.REPLICA_NOT_AVAILABLE, partitionId, leader,
                Optional.empty(), replicaInfo.asJava, isrInfo.asJava, offlineReplicaInfo.asJava)
            } else {
              result += new MetadataResponse.PartitionMetadata(Errors.NONE, partitionId, leader, Optional.of(leaderEpoch),
                replicaInfo.asJava, isrInfo.asJava, offlineReplicaInfo.asJava)
            }
        }
      }
    }
    if (result.isEmpty)
      None
    else
      Option(result)
  }

  private def getAliveEndpoint(snapshot: MetadataSnapshot, brokerId: Int, listenerName: ListenerName): Option[Node] =
    // Returns None if broker is not alive or if the broker does not have a listener named `listenerName`.
    // Since listeners can be added dynamically, a broker with a missing listener could be a transient error.
    snapshot.aliveNodes.get(brokerId).flatMap(_.get(listenerName))

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  def getTopicMetadata(topics: Set[String], listenerName: ListenerName, errorUnavailableEndpoints: Boolean = false,
                       errorUnavailableListeners: Boolean = false): Seq[MetadataResponse.TopicMetadata] = {
    val snapshot = metadataSnapshot
    val topicMetadata = new ArrayBuffer[MetadataResponse.TopicMetadata]
    for (t <- topics) {
      val partitionsMetadata = getPartitionMetadata(snapshot, t, listenerName, errorUnavailableEndpoints, errorUnavailableListeners)
      for (partitionMetadata <- partitionsMetadata) {
        topicMetadata += new MetadataResponse.TopicMetadata(Errors.NONE, t, Topic.isInternal(t), partitionMetadata.toBuffer.asJava)
      }
    }
    topicMetadata
  }

  def getAllTopics(): Set[String] = {
    getAllTopics(metadataSnapshot)
  }

  def getAllPartitions(): Set[TopicPartition] = {
    metadataSnapshot.partitionStates.flatMap { case (topicName, partitionsAndStates) =>
      partitionsAndStates.keys.map(partitionId => new TopicPartition(topicName, partitionId.toInt))
    }.toSet
  }

  private def getAllTopics(snapshot: MetadataSnapshot): Set[String] = {
    snapshot.partitionStates.keySet
  }

  private def getAllPartitions(snapshot: MetadataSnapshot): Map[TopicPartition, UpdateMetadataPartitionState] = {
    snapshot.partitionStates.flatMap { case (topic, partitionStates) =>
      partitionStates.map { case (partition, state ) => (new TopicPartition(topic, partition.toInt), state) }
    }.toMap
  }

  def getNonExistingTopics(topics: Set[String]): Set[String] = {
    topics -- metadataSnapshot.partitionStates.keySet
  }

  def getAliveBroker(brokerId: Int): Option[Broker] = {
    metadataSnapshot.aliveBrokers.get(brokerId)
  }

  def getAliveBrokers: Seq[Broker] = {
    metadataSnapshot.aliveBrokers.values.toBuffer
  }

  private def addOrUpdatePartitionInfo(partitionStates: mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]],
                                       topic: String,
                                       partitionId: Int,
                                       stateInfo: UpdateMetadataPartitionState): Unit = {
    val infos = partitionStates.getOrElseUpdate(topic, mutable.LongMap())
    infos(partitionId) = stateInfo
  }

  def getPartitionInfo(topic: String, partitionId: Int): Option[UpdateMetadataPartitionState] = {
    metadataSnapshot.partitionStates.get(topic).flatMap(_.get(partitionId))
  }

  // if the leader is not known, return None;
  // if the leader is known and corresponding node is available, return Some(node)
  // if the leader is known but corresponding node with the listener name is not available, return Some(NO_NODE)
  def getPartitionLeaderEndpoint(topic: String, partitionId: Int, listenerName: ListenerName): Option[Node] = {
    val snapshot = metadataSnapshot
    snapshot.partitionStates.get(topic).flatMap(_.get(partitionId)) map { partitionInfo =>
      val leaderId = partitionInfo.leader

      snapshot.aliveNodes.get(leaderId) match {
        case Some(nodeMap) =>
          nodeMap.getOrElse(listenerName, Node.noNode)
        case None =>
          Node.noNode
      }
    }
  }

  def getPartitionReplicaEndpoints(tp: TopicPartition, listenerName: ListenerName): Map[Int, Node] = {
    val snapshot = metadataSnapshot
    snapshot.partitionStates.get(tp.topic).flatMap(_.get(tp.partition)).map { partitionInfo =>
      val replicaIds = partitionInfo.replicas
      replicaIds.asScala
        .map(replicaId => replicaId.intValue() -> {
          snapshot.aliveBrokers.get(replicaId.longValue()) match {
            case Some(broker) =>
              broker.getNode(listenerName).getOrElse(Node.noNode())
            case None =>
              Node.noNode()
          }}).toMap
        .filter(pair => pair match {
          case (_, node) => !node.isEmpty
        })
    }.getOrElse(Map.empty[Int, Node])
  }

  def getControllerId: Option[Int] = metadataSnapshot.controllerId

  // [GRR TODO:  sole caller = legacy (non-rewrite) half of doHandleUpdateMetadataRequest() in KafkaApis; clusterId
  //  arg was already there...but might need to be omitted or modified or something?  need to figure out how returned
  //  Cluster part is used (something about client quota callback, so presumably we want clusterId in UMR, not that
  //  of local cluster)]
  def getClusterMetadata(clusterId: String, listenerName: ListenerName): Cluster = {
    val snapshot = metadataSnapshot
    val nodes = snapshot.aliveNodes.map { case (id, nodes) => (id, nodes.get(listenerName).orNull) }
    def node(id: Integer): Node = nodes.get(id.toLong).orNull
    val partitions = getAllPartitions(snapshot)
      .filter { case (_, state) => state.leader != LeaderAndIsr.LeaderDuringDelete }
      .map { case (tp, state) =>
        new PartitionInfo(tp.topic, tp.partition, node(state.leader),
          state.replicas.asScala.map(node).toArray,
          state.isr.asScala.map(node).toArray,
          state.offlineReplicas.asScala.map(node).toArray)
      }
    val unauthorizedTopics = Collections.emptySet[String]
    val internalTopics = getAllTopics(snapshot).filter(Topic.isInternal).asJava
    new Cluster(clusterId, nodes.values.filter(_ != null).toBuffer.asJava,
      partitions.toBuffer.asJava,
      unauthorizedTopics, internalTopics,
      snapshot.controllerId.map(id => node(id)).orNull)
  }

  // This method returns the deleted TopicPartitions received from UpdateMetadataRequest.
  // The key invariant is that the new snapshot cannot affect the old one, i.e., an _unchanged_
  // set of partition states can be reused, but if there are updates, they must go into a
  // completely new map within the new snapshot.  Similarly, federated multi-clusters can
  // reuse the (unchanging) aliveBrokers and aliveNodes sub-maps corresponding to all physical
  // clusters not specified in the update request, but their parent multimaps cannot be reused
  // since one of the physical clusters (the one in the request) always has broker/node changes.
  // [TODO:  add brokerId range-checking to detect when one physical cluster's range overlaps
  //  another's]
  def updateMetadata(correlationId: Int, updateMetadataRequest: UpdateMetadataRequest): Seq[TopicPartition] = {
    inWriteLock(partitionMetadataLock) {

      info(s"GRR DEBUG: entering updateMetadata() (correlationId=${correlationId}, UpdateMetadataRequest clusterId=${updateMetadataRequest.clusterId}, local clusterId=${localClusterId}, federationEnabled=${federationEnabled})")



//FIXME:  is there a better pattern for the following?  this method needs both sets to exist so update of
//  metadataSnapshot at end works, but each pair is pointless overhead for the opposite case (federation vs. not),
//  so initialization to null is the simplest, cheapest approach (no extra Option derefs, etc.) ...
//  alternatively, could use vars and not set until conditional block below, or could accept (minor) code
//  duplication of controllerId/deletedPartitions/partitionStates block and just have completely separate
//  federation vs. non-federation versions of this method ("updatedMultiClusterMetadata()" /
//  "updateSingleClusterMetadata()"):  preferred?  (would avoid multiple checks of federationEnabled boolean,
//  at least...)

      // federation case:  multi-cluster "envelopes" for all clusters' broker/node-maps (keyed by clusterId)
      val multiClusterAliveBrokers = if (federationEnabled)
          new mutable.HashMap[String, mutable.LongMap[Broker]] // (metadataSnapshot.numClusters + 1)   <-- GRR TODO?
          else null
      val multiClusterAliveNodes = if (federationEnabled)
          new mutable.HashMap[String, mutable.LongMap[collection.Map[ListenerName, Node]]] // (metadataSnapshot.numClusters + 1)
          else null

      // legacy data structures for a single physical cluster's brokers/nodes
      val singleClusterAliveBrokers = if (!federationEnabled)
          new mutable.LongMap[Broker](metadataSnapshot.aliveBrokers.size)
          else null
      val singleClusterAliveNodes = if (!federationEnabled)
          new mutable.LongMap[collection.Map[ListenerName, Node]](metadataSnapshot.aliveNodes.size)
          else null


      if (federationEnabled) {

        val mcMetadataSnapshot: MultiClusterMetadataSnapshot = metadataSnapshot.asInstanceOf[MultiClusterMetadataSnapshot]

        // populate each new envelope-map with all clusters' brokers/nodes, except for the cluster in the UMR:
        mcMetadataSnapshot.multiClusterAliveBrokers.keys.foreach { clusterId =>
          if (!clusterId.equals(updateMetadataRequest.clusterId)) {
            info(s"GRR DEBUG: updateMetadata(): copying existing clusterId=${clusterId} brokers to new snapshot since UpdateMetadataRequest is updating clusterId=${updateMetadataRequest.clusterId} brokers")
            multiClusterAliveBrokers(clusterId) = mcMetadataSnapshot.multiClusterAliveBrokers(clusterId)
          } else {
            info(s"GRR DEBUG: updateMetadata(): NOT copying existing clusterId=${clusterId} brokers to new snapshot since UpdateMetadataRequest is replacing those")
          }
        }
        mcMetadataSnapshot.multiClusterAliveNodes.keys.foreach { clusterId =>
          if (!clusterId.equals(updateMetadataRequest.clusterId)) {
            info(s"GRR DEBUG: updateMetadata(): copying existing clusterId=${clusterId} nodes to new snapshot since UpdateMetadataRequest is updating clusterId=${updateMetadataRequest.clusterId} nodes")
            multiClusterAliveNodes(clusterId) = mcMetadataSnapshot.multiClusterAliveNodes(clusterId)
          } else {
            info(s"GRR DEBUG: updateMetadata(): NOT copying existing clusterId=${clusterId} nodes to new snapshot since UpdateMetadataRequest is replacing those")
          }
        }

        // replacement broker- and node-maps for the UpdateMetadataRequest's single physical cluster, which
        // replaces our current copy:
        val umrClusterId = if (updateMetadataRequest.clusterId != null) updateMetadataRequest.clusterId else localClusterId
        val numBrokersInUpdatingCluster = if (mcMetadataSnapshot.multiClusterAliveBrokers.contains(umrClusterId))
            mcMetadataSnapshot.multiClusterAliveBrokers(umrClusterId).size else 0
        val numNodesInUpdatingCluster = if (mcMetadataSnapshot.multiClusterAliveNodes.contains(umrClusterId))
            mcMetadataSnapshot.multiClusterAliveNodes(umrClusterId).size else 0
        val umrAliveBrokers = new mutable.LongMap[Broker](numBrokersInUpdatingCluster)
        val umrAliveNodes = new mutable.LongMap[collection.Map[ListenerName, Node]](numNodesInUpdatingCluster)

        // unconditional replacement of snapshot's broker/node-maps (for a single physical cluster) with those
        // specified in updateMetadataRequest (there's no such thing as delta-updates for a cluster's nodes)
        generateSingleClusterBrokersAndNodesMaps(updateMetadataRequest, umrAliveBrokers, umrAliveNodes)

        multiClusterAliveBrokers(umrClusterId) = umrAliveBrokers
        multiClusterAliveNodes(umrClusterId) = umrAliveNodes

      } else {  // non-federation (legacy/single-cluster) case

        generateSingleClusterBrokersAndNodesMaps(updateMetadataRequest, singleClusterAliveBrokers, singleClusterAliveNodes)

      }




      val controllerId = updateMetadataRequest.controllerId match {
          case id if id < 0 => None
          case id => Some(id)
        }
      val deletedPartitions = new mutable.ArrayBuffer[TopicPartition]
      val possiblyUpdatedPartitionStates = possiblyUpdatePartitionStates(updateMetadataRequest, deletedPartitions, correlationId)

      metadataSnapshot = if (federationEnabled) {
        MultiClusterMetadataSnapshot(possiblyUpdatedPartitionStates, controllerId, multiClusterAliveBrokers, multiClusterAliveNodes)
      } else {
        SingleClusterMetadataSnapshot(possiblyUpdatedPartitionStates, controllerId, singleClusterAliveBrokers, singleClusterAliveNodes)
      }

      deletedPartitions
    }
  }

  def contains(topic: String): Boolean = {
    metadataSnapshot.partitionStates.contains(topic)
  }

  def contains(tp: TopicPartition): Boolean = getPartitionInfo(tp.topic, tp.partition).isDefined

  private def generateSingleClusterBrokersAndNodesMaps(
      updateMetadataRequest: UpdateMetadataRequest,
      aliveBrokers: mutable.LongMap[Broker],
      aliveNodes: mutable.LongMap[collection.Map[ListenerName, Node]]):
      Unit = {
    updateMetadataRequest.liveBrokers.asScala.foreach { broker =>
      // `aliveNodes` is a hot path for metadata requests for large clusters, so we use java.util.HashMap which
      // is a bit faster than scala.collection.mutable.HashMap. When we drop support for Scala 2.10, we could
      // move to `AnyRefMap`, which has comparable performance.
      val nodes = new java.util.HashMap[ListenerName, Node]
      val endPoints = new mutable.ArrayBuffer[EndPoint]
      broker.endpoints.asScala.foreach { ep =>
        val listenerName = new ListenerName(ep.listener)
        endPoints += new EndPoint(ep.host, ep.port, listenerName, SecurityProtocol.forId(ep.securityProtocol))
        nodes.put(listenerName, new Node(broker.id, ep.host, ep.port))
      }
      aliveBrokers(broker.id) = Broker(broker.id, endPoints, Option(broker.rack))
      aliveNodes(broker.id) = nodes.asScala
    }
    aliveNodes.get(brokerId).foreach { listenerMap =>
      val listeners = listenerMap.keySet
      if (!aliveNodes.values.forall(_.keySet == listeners))
        error(s"Listeners are not identical across brokers: $aliveNodes")
    }
  }

  // Conditional replacement of snapshot's partitionStates (might be a full update, a partial update, or no update);
  // called under lock.
  private def possiblyUpdatePartitionStates(
      updateMetadataRequest: UpdateMetadataRequest,
      deletedPartitions: mutable.ArrayBuffer[TopicPartition],
      correlationId: Int):
      mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]] = {
    if (!updateMetadataRequest.partitionStates.iterator.hasNext) {
      metadataSnapshot.partitionStates
    } else {
      //since kafka may do partial metadata updates, we start by copying the previous state
      val partitionStates = new mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]](metadataSnapshot.partitionStates.size)
      metadataSnapshot.partitionStates.foreach { case (topic, oldPartitionStates) =>
        val copy = new mutable.LongMap[UpdateMetadataPartitionState](oldPartitionStates.size)
        copy ++= oldPartitionStates
        partitionStates += (topic -> copy)
      }
//GRR FIXME:  why are/were these two top-level (request) vals not popped out of the loop?  (used solely for trace-level logging, too!)
      val controllerId = updateMetadataRequest.controllerId
      val controllerEpoch = updateMetadataRequest.controllerEpoch
      updateMetadataRequest.partitionStates.asScala.foreach { info =>
        //val controllerId = updateMetadataRequest.controllerId
        //val controllerEpoch = updateMetadataRequest.controllerEpoch
        val tp = new TopicPartition(info.topicName, info.partitionIndex)
        if (info.leader == LeaderAndIsr.LeaderDuringDelete) {
          removePartitionInfo(partitionStates, tp.topic, tp.partition)
//GRR TODO:  for federation case, enhance both of these logs with "cluster ${clusterId} ":
          stateChangeLogger.trace(s"Deleted partition $tp from metadata cache in response to UpdateMetadata " +
            s"request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")
          deletedPartitions += tp
        } else {
          addOrUpdatePartitionInfo(partitionStates, tp.topic, tp.partition, info)
          stateChangeLogger.trace(s"Cached leader info $info for partition $tp in response to UpdateMetadata " +
            s"request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")
        }
      }
      partitionStates
    }
  }

  private def removePartitionInfo(partitionStates: mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]],
                                  topic: String, partitionId: Int): Boolean = {
    partitionStates.get(topic).exists { infos =>
      infos.remove(partitionId)
      if (infos.isEmpty) partitionStates.remove(topic)
      true
    }
  }


  trait MetadataSnapshot {
    def partitionStates(): mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]]
    def controllerId(): Option[Int]
    def aliveBrokers(): mutable.LongMap[Broker]
    def aliveNodes(): mutable.LongMap[collection.Map[ListenerName, Node]]
  }


  case class SingleClusterMetadataSnapshot(
      partitionStatesMap: mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]],
      controllerIdOpt: Option[Int],
      aliveBrokersMap: mutable.LongMap[Broker],
      aliveNodesMap: mutable.LongMap[collection.Map[ListenerName, Node]])
      extends MetadataSnapshot {
    def partitionStates() = partitionStatesMap
    def controllerId() = controllerIdOpt
    def aliveBrokers() = aliveBrokersMap
    def aliveNodes() = aliveNodesMap
  }


  // TODO:  add brokerId ranges (track in updateMetadata()) as sanity check: ensure no overlap between physical clusters
  case class MultiClusterMetadataSnapshot(
      partitionStatesMap: mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]],
      controllerIdOpt: Option[Int],
      multiClusterAliveBrokers: mutable.Map[String, mutable.LongMap[Broker]],
      multiClusterAliveNodes: mutable.Map[String, mutable.LongMap[collection.Map[ListenerName, Node]]])
      extends MetadataSnapshot {

    // GRR VERIFY:  intention is that these things get called exactly once per construction (regardless of getter
    //   calls), since "val" is like "final" and can be set only in ctor...
    val aliveBrokersMap: mutable.LongMap[Broker] = {
      val flattenedBrokersMap: mutable.LongMap[Broker] = new mutable.LongMap[Broker]  // FIXME? could add loop to count total size, or could track it dynamically within snapshots...
      multiClusterAliveBrokers.values.foreach { brokerMap =>
        flattenedBrokersMap ++= brokerMap
      }
      flattenedBrokersMap
    }

    val aliveNodesMap: mutable.LongMap[collection.Map[ListenerName, Node]] = {
      val flattenedNodesMap: mutable.LongMap[collection.Map[ListenerName, Node]] =
          new mutable.LongMap[collection.Map[ListenerName, Node]]  // FIXME? could add loop to count total size, or could track it dynamically within snapshots...
      multiClusterAliveNodes.values.foreach { nodesMap =>
        flattenedNodesMap ++= nodesMap
      }
      flattenedNodesMap
    }

    def partitionStates() = partitionStatesMap
    def controllerId() = controllerIdOpt
    def aliveBrokers() = aliveBrokersMap
    def aliveNodes() = aliveNodesMap
  }

}
