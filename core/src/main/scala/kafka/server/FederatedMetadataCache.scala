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

import kafka.cluster.Broker
import kafka.utils.CoreUtils._
import kafka.utils.Logging
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.requests.UpdateMetadataRequest
import org.apache.kafka.common.{Node, TopicPartition}

import scala.collection.{Seq, mutable}


/**
 *  A cache for the state (e.g., current leader) of each partition and of the federated meta-cluster
 *  (i.e., all brokers in all of the federation's physical clusters). This cache is updated through
 *  UpdateMetadataRequests sent (or forwarded) by the controller. Every broker maintains the same
 *  cache, asynchronously.
 */
class FederatedMetadataCache(brokerId: Int, localClusterId: String = "defaultClusterId") extends MetadataCache(brokerId) with Logging {

  this.metadataSnapshot = MultiClusterMetadataSnapshot(partitionStatesMap = mutable.AnyRefMap.empty,
      controllerIdOpt = None, multiClusterAliveBrokers = mutable.Map.empty, multiClusterAliveNodes = mutable.Map.empty)

  this.logIdent = s"[FederatedMetadataCache brokerId=$brokerId] "


  // This method returns the deleted TopicPartitions received from UpdateMetadataRequest.
  // The key invariant is that the new snapshot cannot affect the old one, i.e., an _unchanged_
  // set of partition states can be reused, but if there are updates, they must go into a
  // completely new map within the new snapshot.  Similarly, federated multi-clusters can
  // reuse the (unchanging) aliveBrokers and aliveNodes sub-maps corresponding to all of the
  // physical clusters NOT specified in the update request, but their parent multimaps cannot
  // be reused since one of the physical clusters (the one in the request) always has broker/node
  // changes.
  // [TODO (LIKAFKA-42886):  add brokerId range-checking to detect when one physical cluster's range
  //  overlaps another's, or compare counts of flattened maps to sum of counts of individual ones:
  //  if mismatch, potentially do set intersections to find duplicate(s)]
  override def updateMetadata(correlationId: Int, updateMetadataRequest: UpdateMetadataRequest): Seq[TopicPartition] = {
    inWriteLock(partitionMetadataLock) {

      info(s"GRR DEBUG: entering federated updateMetadata() (correlationId=${correlationId}, UpdateMetadataRequest clusterId=${updateMetadataRequest.originClusterId}, local clusterId=${localClusterId})")

      // federation case:  multi-cluster "envelopes" for all clusters' broker/node-maps (keyed by clusterId)
      val multiClusterAliveBrokers = new mutable.HashMap[String, mutable.LongMap[Broker]] // (metadataSnapshot.numClusters + 1)   <-- TODO (LIKAFKA-42886)?
      val multiClusterAliveNodes = new mutable.HashMap[String, mutable.LongMap[collection.Map[ListenerName, Node]]] // (metadataSnapshot.numClusters + 1)

      val mcMetadataSnapshot: MultiClusterMetadataSnapshot = metadataSnapshot.asInstanceOf[MultiClusterMetadataSnapshot]

      // populate each new envelope-map with all clusters' brokers/nodes, except for the cluster in the UMR:
      mcMetadataSnapshot.multiClusterAliveBrokers.keys.foreach { clusterId =>
        if (!clusterId.equals(updateMetadataRequest.originClusterId)) {
          info(s"GRR DEBUG: updateMetadata(): copying existing clusterId=${clusterId} brokers to new snapshot since UpdateMetadataRequest is updating clusterId=${updateMetadataRequest.originClusterId} brokers")
          multiClusterAliveBrokers(clusterId) = mcMetadataSnapshot.multiClusterAliveBrokers(clusterId)
        } else {
          info(s"GRR DEBUG: updateMetadata(): NOT copying existing clusterId=${clusterId} brokers to new snapshot since UpdateMetadataRequest is replacing those")
        }
      }
      mcMetadataSnapshot.multiClusterAliveNodes.keys.foreach { clusterId =>
        if (!clusterId.equals(updateMetadataRequest.originClusterId)) {
          info(s"GRR DEBUG: updateMetadata(): copying existing clusterId=${clusterId} nodes to new snapshot since UpdateMetadataRequest is updating clusterId=${updateMetadataRequest.originClusterId} nodes")
          multiClusterAliveNodes(clusterId) = mcMetadataSnapshot.multiClusterAliveNodes(clusterId)
        } else {
          info(s"GRR DEBUG: updateMetadata(): NOT copying existing clusterId=${clusterId} nodes to new snapshot since UpdateMetadataRequest is replacing those")
        }
      }

      // replacement broker- and node-maps for the UpdateMetadataRequest's single physical cluster, which
      // replaces our current copy:
      val umrClusterId = if (updateMetadataRequest.originClusterId != null) updateMetadataRequest.originClusterId else localClusterId
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

      val controllerId = updateMetadataRequest.controllerId match {
          case id if id < 0 => None
          case id => Some(id)
        }
      val deletedPartitions = new mutable.ArrayBuffer[TopicPartition]
      val possiblyUpdatedPartitionStates = possiblyUpdatePartitionStates(updateMetadataRequest, deletedPartitions, correlationId)

      metadataSnapshot = MultiClusterMetadataSnapshot(possiblyUpdatedPartitionStates, controllerId, multiClusterAliveBrokers, multiClusterAliveNodes)

      deletedPartitions
    }
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
