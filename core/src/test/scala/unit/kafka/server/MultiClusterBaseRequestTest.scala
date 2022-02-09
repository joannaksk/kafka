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

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.nio.ByteBuffer
import java.util.Properties

import kafka.api.MultiClusterIntegrationTestHarness
import kafka.network.SocketServer
import kafka.utils.NotNothing
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, RequestHeader, ResponseHeader}

import scala.collection.Seq
import scala.reflect.ClassTag

abstract class MultiClusterBaseRequestTest extends MultiClusterIntegrationTestHarness {
  private var correlationId = 0

  // If required, override properties by mutating the passed Properties object
  protected def brokerPropertyOverrides(properties: Properties): Unit = {}

  // FIXME:  BUG in original commit (cc4fde35c9cc2818af1bcb6861ce32dee0f41677) for original class
  // (BaseRequestTest) from which this one was copied:  does NOT call super.modifyConfigs() =>
  // throwing away IntegrationTestHarness's version => neither setting up listeners nor adding
  // serverConfig => rendering all tests that override serverConfig as (partly) broken:
  //    core/src/test/scala/integration/kafka/api/BaseQuotaTest.scala
  //    core/src/test/scala/integration/kafka/api/ClientIdQuotaTest.scala
  //    core/src/test/scala/integration/kafka/api/ConsumerTopicCreationTest.scala
  //    core/src/test/scala/integration/kafka/api/DelegationTokenEndToEndAuthorizationTest.scala
  //    core/src/test/scala/integration/kafka/api/DescribeAuthorizedOperationsTest.scala
  //    core/src/test/scala/integration/kafka/api/EndToEndAuthorizationTest.scala
  //    core/src/test/scala/integration/kafka/api/EndToEndClusterIdTest.scala
  //    core/src/test/scala/integration/kafka/api/GroupEndToEndAuthorizationTest.scala
  //    core/src/test/scala/integration/kafka/api/MetricsTest.scala
  //    core/src/test/scala/integration/kafka/api/PlaintextEndToEndAuthorizationTest.scala
  //    core/src/test/scala/integration/kafka/api/SaslClientsWithInvalidCredentialsTest.scala
  //    core/src/test/scala/integration/kafka/api/SaslMultiMechanismConsumerTest.scala
  //    core/src/test/scala/integration/kafka/api/SaslPlainPlaintextConsumerTest.scala
  //    core/src/test/scala/integration/kafka/api/SaslPlainSslEndToEndAuthorizationTest.scala
  //    core/src/test/scala/integration/kafka/api/SaslSslAdminIntegrationTest.scala
  //    core/src/test/scala/integration/kafka/api/SaslSslConsumerTest.scala
  //    core/src/test/scala/integration/kafka/api/SslAdminIntegrationTest.scala
  //    core/src/test/scala/integration/kafka/api/SslEndToEndAuthorizationTest.scala
  //    core/src/test/scala/integration/kafka/api/UserClientIdQuotaTest.scala
  //    core/src/test/scala/integration/kafka/api/UserQuotaTest.scala
  //    core/src/test/scala/integration/kafka/api/CustomQuotaCallbackTest.scala
  //    core/src/test/scala/integration/kafka/api/ProxyBasedFederationTest.scala
  //    core/src/test/scala/unit/kafka/server/LogDirFailureTest.scala
  override def modifyConfigs(props: Seq[Properties], clusterIndex: Int): Unit = {
    super.modifyConfigs(props, clusterIndex)
    props.foreach { p =>
      p.put(KafkaConfig.ControlledShutdownEnableProp, "false")
      brokerPropertyOverrides(p)
    }
  }

  def anySocketServer(clusterId: Int): SocketServer = {
    serversByCluster(clusterId).find { server =>
      val state = server.brokerState.currentState
      state != NotRunning.state && state != BrokerShuttingDown.state
    }.map(_.socketServer).getOrElse(throw new IllegalStateException(s"No live broker is available in cluster ${clusterId}"))
  }

  def controllerSocketServer(clusterId: Int): SocketServer = {
    serversByCluster(clusterId).find { server =>
      server.kafkaController.isActive
    }.map(_.socketServer).getOrElse(throw new IllegalStateException(s"No controller broker is available in cluster ${clusterId}"))
  }

  def notControllerSocketServer(clusterId: Int): SocketServer = {
    serversByCluster(clusterId).find { server =>
      !server.kafkaController.isActive
    }.map(_.socketServer).getOrElse(throw new IllegalStateException(s"No non-controller broker is available in cluster ${clusterId}"))
  }

  def brokerSocketServer(clusterId: Int, brokerId: Int): SocketServer = {
    serversByCluster(clusterId).find { server =>
      server.config.brokerId == brokerId
    }.map(_.socketServer).getOrElse(throw new IllegalStateException(s"Could not find broker with id $brokerId in cluster ${clusterId}"))
  }

  def connectAndReceive[T <: AbstractResponse](request: AbstractRequest,
                                               clusterId: Int,
                                               socketServerOpt: Option[SocketServer] = None,
                                               listenerName: ListenerName = listenerName)
                                              (implicit classTag: ClassTag[T], nn: NotNothing[T]): T = {
    val socket = connect(clusterId, socketServerOpt, listenerName)
    try sendAndReceive[T](request, socket)
    finally socket.close()
  }

  def connect(clusterId: Int,
              socketServerOpt: Option[SocketServer] = None,
              listenerName: ListenerName = listenerName): Socket = {
    val socketServer = socketServerOpt.getOrElse {
      anySocketServer(clusterId)
    }
    new Socket("localhost", socketServer.boundPort(listenerName))
  }

  def sendAndReceive[T <: AbstractResponse](request: AbstractRequest,
                                            socket: Socket,
                                            clientId: String = "client-id",
                                            correlationId: Option[Int] = None)
                                           (implicit classTag: ClassTag[T], nn: NotNothing[T]): T = {
    send(request, socket, clientId, correlationId)
    receive[T](socket, request.api, request.version)
  }

  /**
    * Serializes and sends the request to the given api.
    */
  def send(request: AbstractRequest,
           socket: Socket,
           clientId: String = "client-id",
           correlationId: Option[Int] = None): Unit = {
    val header = nextRequestHeader(request.api, request.version, clientId, correlationId)
    sendWithHeader(request, header, socket)
  }

  def nextRequestHeader[T <: AbstractResponse](apiKey: ApiKeys,
                                               apiVersion: Short,
                                               clientId: String = "client-id",
                                               correlationIdOpt: Option[Int] = None): RequestHeader = {
    val correlationId = correlationIdOpt.getOrElse {
      this.correlationId += 1
      this.correlationId
    }
    new RequestHeader(apiKey, apiVersion, clientId, correlationId)
  }

  def sendWithHeader(request: AbstractRequest, header: RequestHeader, socket: Socket): Unit = {
    val serializedBytes = request.serialize(header).array
    sendRequest(socket, serializedBytes)
  }

  private def sendRequest(socket: Socket, request: Array[Byte]): Unit = {
    val outgoing = new DataOutputStream(socket.getOutputStream)
    outgoing.writeInt(request.length)
    outgoing.write(request)
    outgoing.flush()
  }

  def receive[T <: AbstractResponse](socket: Socket, apiKey: ApiKeys, version: Short)
                                    (implicit classTag: ClassTag[T], nn: NotNothing[T]): T = {
    val incoming = new DataInputStream(socket.getInputStream)
    val len = incoming.readInt()

    val responseBytes = new Array[Byte](len)
    incoming.readFully(responseBytes)

    val responseBuffer = ByteBuffer.wrap(responseBytes)
    ResponseHeader.parse(responseBuffer, apiKey.responseHeaderVersion(version))

    val responseStruct = apiKey.parseResponse(version, responseBuffer)
    AbstractResponse.parseResponse(apiKey, responseStruct, version) match {
      case response: T => response
      case response =>
        throw new ClassCastException(s"Expected response with type ${classTag.runtimeClass}, but found ${response.getClass}")
    }
  }

}
