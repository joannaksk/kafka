/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the
 * License.
 */

package kafka.api

import scala.collection.JavaConverters._

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.junit.Assert._
import org.junit.{Before, Test}

import kafka.server.KafkaConfig
import kafka.server.KafkaServer
import kafka.server.QuotaType
import kafka.utils.TestUtils
import org.apache.kafka.common.{KafkaException, Node}

/**
 * Currently a simple proof of concept of a multi-cluster integration test, but ultimately intended
 * as a feasibility test for proxy-based federation:  specifically, can a vanilla client (whether
 * consumer, producer, or admin) talk to a federated set of two or more physical clusters and
 * successfully execute all parts of its API?
 */
class ProxyBasedFederationTest extends MultiClusterAbstractConsumerTest {
  override def numClusters: Int = 2  // need one ZK instance for each Kafka cluster  [TODO: can we "chroot" instead?]
  override def brokerCountPerCluster: Int = 5  // 5 _per Kafka cluster_ (3 normal, 2 controllers), i.e., 10 total

  @Test
  def testBasicMultiClusterSetup(): Unit = {
    debug(s"\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n") // GRR DEBUG
    debug(s"GRR DEBUG:  beginning testBasicMultiClusterSetup() with numClusters=${numClusters} and brokerCountPerCluster=${brokerCountPerCluster}")
    val numRecords = 1000
    debug(s"GRR DEBUG:  creating producer (IMPLICITLY FOR CLUSTER 0)")
    val producer = createProducer()
    debug(s"GRR DEBUG:  using producer to send $numRecords records to topic-partition $tp1c0 (IMPLICITLY FOR CLUSTER 0)")
    sendRecords(producer, numRecords, tp1c0)

    debug(s"GRR DEBUG:  creating consumer (IMPLICITLY FOR CLUSTER 0)")
    val consumer = createConsumer()
    debug(s"GRR DEBUG:  'assigning' consumer to topic-partition $tp1c0 ... or vice-versa (IMPLICITLY FOR CLUSTER 0)")
    consumer.assign(List(tp1c0).asJava)
    debug(s"GRR DEBUG:  seeking to beginning of topic-partition $tp1c0 (IMPLICITLY FOR CLUSTER 0)")
    consumer.seek(tp1c0, 0)
    debug(s"GRR DEBUG:  calling consumeAndVerifyRecords()")
    consumeAndVerifyRecords(consumer = consumer, numRecords = numRecords, startingOffset = 0)
    debug(s"GRR DEBUG:  done with testBasicMultiClusterSetup()\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
  }




/*
   GRR FIXME:
    -----------------------------------------------------------------------------------------------------------------
    x [OUTSIDE OF TEST] must define, read, and obey new federation config(s):  at LEAST "federation.enable" = true
      - ideally also some kind of "federated.cluster.id" or whatever that all physical clusters in federation
        can share => know that they're in the same federation (or is that naive?  ideally want auto-discovery
        somehow, but since not sharing same ZK cluster, unclear how that would work:  asked Nick what would be
        ideal in his eyes)
      - [longer-term] ideally also a persistent "color" per physical cluster so can log things more memorably
 
      x in federation test:
          // want federation.enable to be universal => can/should add it to serverConfig map/hashtable
          // (BUT IF AND ONLY IF WE FIX modifyConfigs() OVERRIDE DEFINITION!) [FIXED]
          this.serverConfig.setProperty("federation.enable", "true")  // or KafkaConfig.FederationEnableProp
          // existing (relevant) configs to set in test:
          //   KafkaConfig.LiCombinedControlRequestEnableProp = "li.combined.control.request.enable"
          //   false for now; should be global => serverConfig
          this.serverConfig.setProperty(KafkaConfig.LiCombinedControlRequestEnableProp, "false")
          //   KafkaConfig.AllowPreferredControllerFallbackProp = "allow.preferred.controller.fallback"
          //   false for now; should be global => serverConfig
          this.serverConfig.setProperty(KafkaConfig.AllowPreferredControllerFallbackProp, "false")
      x in KafkaConfig:
          // also add KafkaConfig.LiFederationEnableProp == "li.federation.enable" to KafkaConfig so "fromProp()"
          // works on it
      x in KafkaController, channel mgr, and/or KafkaBroker:
          // also add to controller and maybe broker code so routing works right:  enable iff federation.enable == true
 
          // this.serverConfig.setProperty(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required")
          // this.serverConfig.setProperty(KafkaConfig.AuthorizerClassNameProp, classOf[SimpleAclAuthorizer].getName)
          // this.serverConfig.setProperty(KafkaConfig.DeleteTopicEnableProp, "true")
          // this.serverConfig.setProperty(KafkaConfig.ProducerQuotaBytesPerSecondDefaultProp, Long.MaxValue.toString)
          // this.serverConfig.setProperty(s"${listenerName.configPrefix}${KafkaConfig.PrincipalBuilderClassProp}",

    -----------------------------------------------------------------------------------------------------------------
    x need way to set configs of test brokers, including brokerIds of all brokers (must be unique in federation)
      and preferred controller-ness of two brokers in each cluster
      >>>>>>>>>>> should be in MultiClusterKafkaServerTestHarness:  servers, instanceConfigs, generateConfigs()
      >>>>>>>>>>> some overrides in MultiClusterIntegrationTestHarness, too
        
      x YES:  can just override modifyConfigs() in test:
        // MultiClusterIntegrationTestHarness (ultimate superclass):
        override def modifyConfigs(props: Seq[Properties], clusterIndex: Int): Unit = {
          configureListeners(props)
          props.foreach(_ ++= serverConfig)
        }
        // MultiClusterBaseRequestTest (superclass of test, subclass of MultiClusterIntegrationTestHarness):
        override def modifyConfigs(props: Seq[Properties], clusterIndex: Int): Unit = {
          super.modifyConfigs(props, clusterIndex)
          props.foreach { p =>
            p.put(KafkaConfig.ControlledShutdownEnableProp, "false")
            brokerPropertyOverrides(p)		// multiple (non-MultiCluster) tests do override this per-broker method
          }
        }
 
        x need to add clusterIndex arg (doh!), but that's fine:  SaslPlainPlaintextConsumerTest overrides
          method, but it does the non-MultiCluster one; within MultiCluster-land, only MultiClusterBaseRequestTest
          does so, and we can fix that:  DO IT!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 
      x existing (relevant) configs to set in test:
        x KafkaConfig.BrokerIdProp = "broker.id"
          // 100*(clusterIndex + 1) + {0 .. 4}
        x KafkaConfig.PreferredControllerProp = "preferred.controller"
          // true for brokerIds 100, 101, 200, 201 only
 
      X hack up TestUtils.bootstrapServers() to tell each of instanceServers (brokers) what its bootstrap value
        is, and hack up latter to let us look up that info (if KafkaServer and/or KafkaBroker doesn't already know)
        x NO, not necessary:  each controller (channel mgr) already has brokerStateInfo map of brokerId ->
          ControllerBrokerStateInfo, and latter has NetworkClient (which, logging shows, knows host:port),
          "brokerNode" (== Broker node() result, whatever that is), and RequestSendThread:  SOMEWHERE in
          there must be way to get host:port back out, and if not, can always hack ControllerBrokerStateInfo
          to include Broker and/or host + port directly
        x with all that ^^^^, get add a new "getSelfBroker()" method or "getConnectionInfo()" or something
          (also a test-hack) to KafkaServer, which test itself can use either at top of test case or bottom
          of setUp() to pass to OTHER cluster's controllers, e.g.:
             // In real life this would be done via configs, but since we're running MANY brokers on the same
             // host, we don't know the port numbers ahead of time, i.e., they're dynamically generated at
             // runtime.  Ergo, we look them up after everybody has started and cross-pollinate via setters:
             val serversInCluster0 = serversByCluster(0)
             val serversInCluster1 = serversByCluster(1)
             // inform both preferred controllers in cluster 0 of both remote controllers (i.e., in cluster 1)
             serversInCluster0(0).addRemoteController(serversInCluster1(0).getConnectionInfo())
             serversInCluster0(1).addRemoteController(serversInCluster1(1).getConnectionInfo())
             // inform both preferred controllers in cluster 1 of both remote controllers (i.e., in cluster 0)
             serversInCluster1(0).addRemoteController(serversInCluster0(0).getConnectionInfo())
             serversInCluster1(1).addRemoteController(serversInCluster0(1).getConnectionInfo())
 
      x clone addNewBroker() or modify it to include remote controllers (with filter to prevent remote UMRs from
        heading back out to remote controllers)

    -----------------------------------------------------------------------------------------------------------------
    - [ideally] need way to associate clusterIndex with (generated only?) clusterId of each cluster

    -----------------------------------------------------------------------------------------------------------------
    x need way to specify which are the preferred controllers of the _other_ physical cluster in both clusters
      X for test (only), could set up controllersByCluster() method + backing array in
        MultiClusterIntegrationTestHarness that enables easy lookup for us...  would still need to inject
        into actual broker/controller code somehow, but would avoid config dependency and possible race
        (i.e., don't know port numbers of brokers until they start, and can't leave configs for after startup
        => can't pre-configure remote controllers in configs)
      x no, can just add some accessors and let test case (or setUp()) do so directly

    -----------------------------------------------------------------------------------------------------------------
    x need way to create at least one topic in each physical cluster (maybe 3 and 5 partitions, respectively?)
      >>>>>>>>>>> should be in MultiClusterKafkaServerTestHarness, MultiClusterIntegrationTestHarness
      - TopicWith3Partitions
      - TopicWith5Partitions

GRR WORKING:
    -----------------------------------------------------------------------------------------------------------------
    - need way to hook up producer and/or consumer client to either physical cluster initially

    -----------------------------------------------------------------------------------------------------------------
    - need way to verify all setup
      - among other things, want to enumerate all topics in each physical cluster and verify NO OVERLAP

    -----------------------------------------------------------------------------------------------------------------
    - need way to verify remote UMRs are sent/received in both directions

    -----------------------------------------------------------------------------------------------------------------
    - FIXME:  MultiClusterIntegrationTestHarness creates consumer-offsets topic in each cluster, but need
      to eliminate that if federation.enable == true

        +---------------------------------------+    +---------------------------------------+
        | PHYSICAL CLUSTER INDEX 0              |    | PHYSICAL CLUSTER INDEX 1              |
        |  - broker 100 = preferred controller  |    |  - broker 200 = preferred controller  |
        |  - broker 101 = preferred controller  |    |  - broker 201 = preferred controller  |
        |  - broker 102 = data broker           |    |  - broker 202 = data broker           |
        |  - broker 103 = data broker/bootstrap |    |  - broker 203 = data broker/bootstrap |
        |  - broker 104 = data broker           |    |  - broker 204 = data broker           |
        +---------------------------------------+    +---------------------------------------+

REF:
 - super to subclasses:
   - MultiClusterZooKeeperTestHarness
        protected def numClusters: Int = 1
        [GRR changes:
          (1) added Buffer[]-wrapped zkClients, adminZkClients, zookeepers (all new vals)
          (2) added backward-compatible accessor methods to replace zkClient, adminZkClient, zookeeper, zkConnect vars
          (3) added extended, same-name-but-one-arg accessor methods to index zkClient, adminZkClient, zookeeper,
              zkConnect by cluster (i.e., to access "other dimension" added by Buffer[]-wrapping them, basically)
          (4) extended setUp() to create numClusters instances of zkClient, adminZkClient, zookeeper rather than
              just one
          (5) extended tearDown() to loop over numClusters instances of zkClients and zookeepers rather than just one
        ]
   - MultiClusterKafkaServerTestHarness
        Buffer[KafkaServer] servers             [created in setUp() => can potentially override]
        Seq[KafkaConfig] instanceConfigs
        configs() that calls generateConfigs() [abstract here!] if not already generated
        setUp() that creates servers
        createTopic() methods x 2
        [GRR changes:
          (1) converted instanceConfigs var to Buffer[]-wrapped val
          (2) converted servers var to Buffer[]-wrapped val "instanceServers"
          (3) converted brokerList var to Buffer[]-wrapped val "brokerLists"
          (4) added backward-compatible accessor methods to replace servers and brokerList vars
          (5) added final but otherwise backward-compatible accessor method to replace configs method
          (6) added extended, same-name-but-one-arg accessor method to index brokerList by cluster
          (7) added extended, NOT-same-name-but-one-arg accessor methods to index "servers" and "configs" by cluster
              (specifically, serversByCluster() and configsByCluster(), with latter having special logic to avoid
              breaking existing overrides of generateConfigs() abstract method for clusterId == 0 case)
          (8) added extended, NOT-same-name-but-one-arg, NOT-quite-abstract method to generate configs for various
              clusters (namely, generateConfigsByCluster(Int), which devolves to generateConfigs() for Int == 0 but
              throws for non-zero values, i.e., multi-cluster implementations must override it but single-cluster
              ones need not)
          (9) extended setUp() to create numClusters instances of instanceServers array and brokerList string
              [does NOT yet handle "alive" array correctly]
         (10) extended tearDown() to loop over numClusters instances of "servers" (i.e., serversByCluster(i))
         (11) extended first createTopic() method with extra clusterIndex arg at end (defaulting to 0)
         (12) extended second createTopic() method with extra clusterIndex arg at end (NO default, sigh => code changes!)
         (13) [NO changes to killRandomBroker(), killBroker(), or restartDeadBrokers():  all still single-cluster]
        ]
   - MultiClusterIntegrationTestHarness
        protected def brokerCount: Int
        sole override/impl of generateConfigs() in test-harness stack (but mult tests override)
        [GRR changes:
          (1) added extended, NOT-same-name-but-one-arg override implementation for generateConfigsByCluster(Int)
              (and switched implementation of existing no-args generateConfigs() to call it with arg == 0)
          (2) extended setUp() to create numClusters instances of offsets topic
              [does NOT yet extend producerConfig, consumerConfig, and adminClientConfig to extra clusters]
          (3) [NO changes to createProducer(), createConsumer(), or createAdminClient():  all still single-cluster]
        ]
   - MultiClusterBaseRequestTest
        override def brokerCount: Int = 3
        override def modifyConfigs(props: Seq[Properties]): Unit = { ... }
        [provides SocketServer stuff, connect/send/receive, etc.:  IMPORTANT]
   - MultiClusterAbstractConsumerTest
        override def brokerCount: Int = 3
        CALLS createTopic(topic, 2, brokerCount) in setUp() (sole addition to super.setUp())
        [provides client send/receive stuff, commit callback, assignments/CGM, etc.:  IMPORTANT]
   - ProxyBasedFederationTest
        override def numClusters: Int = 2
        override def brokerCountPerCluster: Int = 5  (was 3 originally, but now want dedicated controllers, too)
 */



  // Set up brokers and controllers as follows:
  //    +---------------------------------------+    +---------------------------------------+
  //    | PHYSICAL CLUSTER(-INDEX) 0            |    | PHYSICAL CLUSTER(-INDEX) 1            |
  //    |  - broker 100 = preferred controller  |    |  - broker 200 = preferred controller  |
  //    |  - broker 101 = preferred controller  |    |  - broker 201 = preferred controller  |
  //    |  - broker 102 = data broker           |    |  - broker 202 = data broker           |
  //    |  - broker 103 = data broker           |    |  - broker 203 = data broker           |
  //    |  - broker 104 = data broker           |    |  - broker 204 = data broker           |
  //    +---------------------------------------+    +---------------------------------------+
  // The bootstrap-servers list for each cluster will contain all five brokers.
  override def modifyConfigs(props: Seq[Properties], clusterIndex: Int): Unit = {
    debug(s"GRR DEBUG:  beginning ProxyBasedFederationTest modifyConfigs() override for clusterIndex=${clusterIndex}")
    super.modifyConfigs(props, clusterIndex)
    (0 until brokerCountPerCluster).map { brokerIndex =>
      // 100-104, 200-204
      val brokerId = 100*(clusterIndex + 1) + brokerIndex
      debug(s"GRR DEBUG:  clusterIndex=${clusterIndex}, brokerIndex=${brokerIndex}: setting broker.id=${brokerId}")
      props(brokerIndex).setProperty(KafkaConfig.BrokerIdProp, brokerId.toString)
      if (brokerIndex < 2) {
        // true for brokerIds 100, 101, 200, 201 only
        debug(s"GRR DEBUG:  clusterIndex=${clusterIndex}, brokerIndex=${brokerIndex}, broker.id=${brokerId}: setting preferred.controller=true")
        props(brokerIndex).setProperty(KafkaConfig.PreferredControllerProp, "true")
      } else {
        debug(s"GRR DEBUG:  clusterIndex=${clusterIndex}, brokerIndex=${brokerIndex}, broker.id=${brokerId}: leaving preferred.controller=false")
      }
    }
    debug(s"GRR DEBUG:  done with ProxyBasedFederationTest modifyConfigs() override for clusterIndex=${clusterIndex}\n\n\n\n\n")
  }


  @Before
  override def setUp(): Unit = {
    debug(s"GRR DEBUG:  beginning setUp() override for ProxyBasedFederationTest to enable federation, disable combined control requests, etc.\n\n\n\n\n")
    this.serverConfig.setProperty(KafkaConfig.LiFederationEnableProp, "true")
    this.serverConfig.setProperty(KafkaConfig.LiCombinedControlRequestEnableProp, "false")
    this.serverConfig.setProperty(KafkaConfig.AllowPreferredControllerFallbackProp, "false")
    this.serverConfig.setProperty(KafkaConfig.BrokerIdGenerationEnableProp, "false")
    this.serverConfig.setProperty(KafkaConfig.AutoCreateTopicsEnableProp, "false")
    super.setUp()
    // ^^^^^^^^^ GRR FIXME:  to deal with consumer-offsets topic, should replace that with doSetup() call
    //   (which exists at least in MultiClusterIntegrationTestHarness), which then invokes its own super.setUp()
    //   before setting up client configs and optionally creating offsets topic(s) ... oh, wait, we don't have
    //   a way to make it true for one cluster and false for the other, so maybe should force to false and
    //   then create it manually?  NEEDS MORE INVESTIGATION

    debug(s"GRR DEBUG:  checking out both controllers' data structures (still in setUp()!)\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")

    // In real life this would be done via configs, but since we're running MANY brokers on the same
    // host, we don't know the port numbers at config-time, i.e., they're dynamically generated at
    // runtime.  Ergo, we look them up after everybody has started and cross-pollinate via setters:

//GRR FIXME:  this is a mess...we need a MUCH more elegant way to figure out the two leader-controllers and tell
//  them about one another (at a minimum, can we just do some if/else instead of nested match blocks?  ugh..)

    val serversInCluster0 = serversByCluster(0)
    var cluster0ControllerIndex = 0
    var cluster0ControllerId = 100      // 100 should be leader...
    var cluster0ControllerNodeOpt = serversInCluster0(cluster0ControllerIndex).getBrokerNode(cluster0ControllerId)
    var cluster0ControllerNode: Node = null
    cluster0ControllerNodeOpt match {
      case Some(node) =>
        cluster0ControllerNode = node
      case None =>
        cluster0ControllerIndex = 1
        cluster0ControllerId = 101      // ...but if not, it better be 101
        cluster0ControllerNodeOpt = serversInCluster0(cluster0ControllerIndex).getBrokerNode(cluster0ControllerId)
        cluster0ControllerNodeOpt match {
          case Some(node2) =>
            cluster0ControllerNode = node2
          case None =>
            throw new KafkaException(s"neither preferred controller in cluster 0 has info about itself")
        }
    }

    val serversInCluster1 = serversByCluster(1)
    var cluster1ControllerIndex = 0
    var cluster1ControllerId = 200      // 200 should be leader...
    var cluster1ControllerNodeOpt = serversInCluster1(cluster1ControllerIndex).getBrokerNode(cluster1ControllerId)
    var cluster1ControllerNode: Node = null
    cluster1ControllerNodeOpt match {
      case Some(node) =>
        cluster1ControllerNode = node
      case None =>
        cluster1ControllerIndex = 1
        cluster1ControllerId = 201      // ...but if not, it better be 201
        cluster1ControllerNodeOpt = serversInCluster1(cluster1ControllerIndex).getBrokerNode(cluster1ControllerId)
        cluster1ControllerNodeOpt match {
          case Some(node2) =>
            cluster1ControllerNode = node2
          case None =>
            throw new KafkaException(s"neither preferred controller in cluster 1 has info about itself")
        }
    }

    info(s"GRR DEBUG:  cluster 0:  server(${cluster0ControllerIndex}) is the lead controller (controllerId=${cluster0ControllerId}); creating new Broker object from its own Node info (id=${cluster0ControllerNode.id}, host=${cluster0ControllerNode.host}, port=${cluster0ControllerNode.port}) and passing to cluster 1's controller")
    val cluster0ControllerBroker = TestUtils.createBroker(cluster0ControllerNode.id, cluster0ControllerNode.host, cluster0ControllerNode.port)
    serversInCluster1(cluster1ControllerIndex).addRemoteController(cluster0ControllerBroker)
    // no need to notify cluster 1 of our other controller since it's definitely not tracking its own brokers
    // and presumably won't pay attention (as a controller) to the remote ones, either

    info(s"GRR DEBUG:  cluster 1:  server(${cluster1ControllerIndex}) is the lead controller (controllerId=${cluster1ControllerId}); creating new Broker object from its own Node info (id=${cluster1ControllerNode.id}, host=${cluster1ControllerNode.host}, port=${cluster1ControllerNode.port}) and passing to cluster 0's controller")
    val cluster1ControllerBroker = TestUtils.createBroker(cluster1ControllerNode.id, cluster1ControllerNode.host, cluster1ControllerNode.port)
    serversInCluster0(cluster0ControllerIndex).addRemoteController(cluster1ControllerBroker)
    // no need to notify cluster 0 of our other controller for the same reason


    debug(s"GRR DEBUG:  done with setUp() override for ProxyBasedFederationTest\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
  }



  /**
   * Create topics in both physical clusters, create a producer for one of them and produce 1000 records to it,
   * create a consumer for both physical clusters, have both of them consume the cluster-0 topic's records, and
   * verify the same.
   */
  @Test
  def testFederatedClients(): Unit = {
    debug(s"GRR DEBUG:  beginning testFederatedClients() with numClusters=${numClusters} and brokerCountPerCluster=${brokerCountPerCluster}")

    debug(s"\n\n\n\t\tGRR DEBUG:  creating dual-partition topic '${topicNameCluster0}' in cluster 0:  THIS SHOULD TRIGGER CROSS-CLUSTER UpdateMetadataRequest\n\n")
    createTopic(topicNameCluster0, numPartitions = 2, replicaCount, clusterIndex = 0)

    debug(s"GRR DEBUG:  creating admin client for cluster 0")
    val cluster0AdminClient = createAdminClient(clusterIndex = 0)
    debug(s"GRR DEBUG:  requesting list of topics in federation using admin client for cluster 0 ...")
    var topicsViaCluster0 = cluster0AdminClient.listTopics().names().get(10000, TimeUnit.MILLISECONDS)
    debug(s"\n\n\n\t\tGRR DEBUG:  federated topics list via broker in physical cluster 0 = ${topicsViaCluster0}\n\n")

    debug(s"GRR DEBUG:  creating admin client for cluster 1")
    val cluster1AdminClient = createAdminClient(clusterIndex = 1)
    debug(s"GRR DEBUG:  requesting list of topics in federation using admin client for cluster 1 ...")
    var topicsViaCluster1 = cluster1AdminClient.listTopics().names().get(10000, TimeUnit.MILLISECONDS)
    debug(s"\n\n\n\t\tGRR DEBUG:  federated topics list via broker in physical cluster 1 = ${topicsViaCluster1}\n\n")


    debug(s"\n\n\n\t\tGRR DEBUG:  creating single-partition topic '${topicNameCluster1}' in cluster 1:  THIS SHOULD TRIGGER CROSS-CLUSTER UpdateMetadataRequest\n\n")
    createTopic(topicNameCluster1, numPartitions = 1, replicaCount, clusterIndex = 1)  // single-partition topic in 2nd cluster for simplicity

    debug(s"GRR DEBUG:  again requesting list of topics in federation using admin client for cluster 0 ...")
    topicsViaCluster0 = cluster0AdminClient.listTopics().names().get(10000, TimeUnit.MILLISECONDS)
    debug(s"\n\n\n\t\tGRR DEBUG:  updated federated topics list via broker in physical cluster 0 = ${topicsViaCluster0}\n\n")

    debug(s"GRR DEBUG:  again requesting list of topics in federation using admin client for cluster 1 ...")
    topicsViaCluster1 = cluster1AdminClient.listTopics().names().get(10000, TimeUnit.MILLISECONDS)
    debug(s"\n\n\n\t\tGRR DEBUG:  updated federated topics list via broker in physical cluster 1 = ${topicsViaCluster1}\n\n")


    val numRecs = 1000
    debug(s"GRR DEBUG:  creating producer for cluster 0")
    val producer = createProducer(clusterIndex = 0)
    debug(s"GRR DEBUG:  using producer to send $numRecs records to topic-partition $tp1c0 in cluster 0")
    sendRecords(producer, numRecs, tp1c0)


    debug(s"\n\n\n\t\tGRR DEBUG:  creating consumer for cluster 0 to consume from $tp1c0\n\n")
    val cluster0Consumer = createConsumer(clusterIndex = 0)
    debug(s"GRR DEBUG:  'assigning' consumer to topic-partition $tp1c0 in cluster 0")
    cluster0Consumer.assign(List(tp1c0).asJava)
    debug(s"GRR DEBUG:  seeking consumer to beginning of topic-partition $tp1c0 in cluster 0")
    cluster0Consumer.seek(tp1c0, 0)
    debug(s"GRR DEBUG:  calling consumeAndVerifyRecords() for consumer in cluster 0")
    consumeAndVerifyRecords(consumer = cluster0Consumer, tp = tp1c0, numRecords = numRecs, startingOffset = 0)


    debug(s"\n\n\n\t\tGRR DEBUG:  creating consumer for cluster 1 to consume from $tp1c0\n\n")
    val cluster1Consumer = createConsumer(clusterIndex = 1)
    debug(s"GRR DEBUG:  'assigning' cluster-1 consumer to topic-partition $tp1c0 in cluster 0")
    cluster1Consumer.assign(List(tp1c0).asJava)
    debug(s"GRR DEBUG:  seeking cluster-1 consumer to beginning of topic-partition $tp1c0 in cluster 0")
    cluster1Consumer.seek(tp1c0, 0)
    debug(s"GRR DEBUG:  calling consumeAndVerifyRecords() for consumer in cluster 1")
    consumeAndVerifyRecords(consumer = cluster1Consumer, tp = tp1c0, numRecords = numRecs, startingOffset = 0)


    debug(s"GRR DEBUG:  done with functional parts; now running assertions for consumer in cluster 0")

    // this stuff is just ripped off from PlaintextConsumerTest testQuotaMetricsNotCreatedIfNoQuotasConfigured()
    def assertNoMetric(broker: KafkaServer, name: String, quotaType: QuotaType, clientId: String): Unit = {
        val metricName = broker.metrics.metricName(name,
                                  quotaType.toString,
                                  "",
                                  "user", "",
                                  "client-id", clientId)
        assertNull("Metric should not have been created " + metricName, broker.metrics.metric(metricName))
    }
    serversByCluster(0).foreach(assertNoMetric(_, "byte-rate", QuotaType.Produce, producerClientId))
    serversByCluster(0).foreach(assertNoMetric(_, "throttle-time", QuotaType.Produce, producerClientId))
    serversByCluster(0).foreach(assertNoMetric(_, "byte-rate", QuotaType.Fetch, consumerClientId))
    serversByCluster(0).foreach(assertNoMetric(_, "throttle-time", QuotaType.Fetch, consumerClientId))

    serversByCluster(0).foreach(assertNoMetric(_, "request-time", QuotaType.Request, producerClientId))
    serversByCluster(0).foreach(assertNoMetric(_, "throttle-time", QuotaType.Request, producerClientId))
    serversByCluster(0).foreach(assertNoMetric(_, "request-time", QuotaType.Request, consumerClientId))
    serversByCluster(0).foreach(assertNoMetric(_, "throttle-time", QuotaType.Request, consumerClientId))

    debug(s"GRR DEBUG:  done with testFederatedClients()\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
  }

}
