/*
 * Copyright 2019 is-land
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.island.ohara.client.configurator.v0

import java.util.Objects

import com.island.ohara.client.configurator.v0.InfoApi.ConnectorVersion
import com.island.ohara.client.configurator.v0.JarApi._
import com.island.ohara.common.annotations.Optional
import com.island.ohara.common.util.{CommonUtils, VersionUtils}
import com.island.ohara.kafka.connector.json.SettingDefinition
import spray.json.DefaultJsonProtocol._
import spray.json.{DeserializationException, JsArray, JsNull, JsNumber, JsObject, JsString, JsValue, RootJsonFormat}

import scala.concurrent.{ExecutionContext, Future}
object WorkerApi {
  val WORKER_PREFIX_PATH: String = "workers"

  /**
    * the default docker image used to run containers of worker cluster
    */
  val IMAGE_NAME_DEFAULT: String = s"oharastream/connect-worker:${VersionUtils.VERSION}"

  private[this] val NAME_KEY = "name"
  private[this] val IMAGE_NAME_KEY = "imageName"
  private[this] val BROKER_CLUSTER_NAME_KEY = "brokerClusterName"
  private[this] val CLIENT_PORT_KEY = "clientPort"
  private[this] val JMX_PORT_KEY = "jmxPort"
  private[this] val GROUP_ID_KEY = "groupId"
  private[this] val STATUS_TOPIC_NAME_KEY = "statusTopicName"
  private[this] val STATUS_TOPIC_PARTITIONS_KEY = "statusTopicPartitions"
  private[this] val STATUS_TOPIC_REPLICATIONS_KEY = "statusTopicReplications"
  private[this] val CONFIG_TOPIC_NAME_KEY = "configTopicName"
  private[this] val CONFIG_TOPIC_PARTITIONS_KEY = "configTopicPartitions"
  private[this] val CONFIG_TOPIC_REPLICATIONS_KEY = "configTopicReplications"
  private[this] val OFFSET_TOPIC_NAME_KEY = "offsetTopicName"
  private[this] val OFFSET_TOPIC_PARTITIONS_KEY = "offsetTopicPartitions"
  private[this] val OFFSET_TOPIC_REPLICATIONS_KEY = "offsetTopicReplications"
  private[this] val JAR_IDS_KEY = "jarIds"
  private[this] val JAR_INFOS_KEY = "jarInfos"
  // TODO: deprecated key
  private[this] val JARS_KEY = "jars"
  private[this] val JAR_NAMES_KEY = "jarNames"
  private[this] val CONNECTORS_KEY = "connectors"
  // TODO: deprecated key
  private[this] val SOURCES_KEY = "sources"
  // TODO: deprecated key
  private[this] val SINKS_KEY = "sinks"
  private[this] val NODE_NAMES_KEY = "nodeNames"

  final case class Creation private[WorkerApi] (name: String,
                                                imageName: String,
                                                brokerClusterName: Option[String],
                                                clientPort: Int,
                                                jmxPort: Int,
                                                groupId: String,
                                                configTopicName: String,
                                                // configTopicPartitions must be 1
                                                configTopicReplications: Short,
                                                offsetTopicName: String,
                                                offsetTopicPartitions: Int,
                                                offsetTopicReplications: Short,
                                                statusTopicName: String,
                                                statusTopicPartitions: Int,
                                                statusTopicReplications: Short,
                                                jarIds: Set[String],
                                                nodeNames: Set[String])
      extends ClusterCreationRequest {
    override def ports: Set[Int] = Set(clientPort, jmxPort)
  }

  /**
    * exposed to configurator
    */
  private[ohara] implicit val WORKER_CLUSTER_CREATION_REQUEST_JSON_FORMAT: RootJsonFormat[Creation] =
    new RootJsonFormat[Creation] {
      override def write(obj: Creation): JsValue = JsObject(
        noJsNull(
          Map(
            NAME_KEY -> JsString(obj.name),
            IMAGE_NAME_KEY -> JsString(obj.imageName),
            BROKER_CLUSTER_NAME_KEY -> obj.brokerClusterName.map(JsString(_)).getOrElse(JsNull),
            CLIENT_PORT_KEY -> JsNumber(obj.clientPort),
            JMX_PORT_KEY -> JsNumber(obj.jmxPort),
            GROUP_ID_KEY -> JsString(obj.groupId),
            STATUS_TOPIC_NAME_KEY -> JsString(obj.statusTopicName),
            STATUS_TOPIC_PARTITIONS_KEY -> JsNumber(obj.statusTopicPartitions),
            STATUS_TOPIC_REPLICATIONS_KEY -> JsNumber(obj.statusTopicReplications),
            CONFIG_TOPIC_NAME_KEY -> JsString(obj.configTopicName),
            CONFIG_TOPIC_REPLICATIONS_KEY -> JsNumber(obj.configTopicReplications),
            OFFSET_TOPIC_NAME_KEY -> JsString(obj.offsetTopicName),
            OFFSET_TOPIC_PARTITIONS_KEY -> JsNumber(obj.statusTopicPartitions),
            OFFSET_TOPIC_REPLICATIONS_KEY -> JsNumber(obj.offsetTopicReplications),
            JAR_IDS_KEY -> JsArray(obj.jarIds.map(JsString(_)).toVector),
            NODE_NAMES_KEY -> JsArray(obj.nodeNames.map(JsString(_)).toVector)
          ))
      )

      override def read(json: JsValue): Creation = try {
        val groupId = noJsNull(json).get(GROUP_ID_KEY).map(_.convertTo[String]).getOrElse(CommonUtils.randomString(10))
        Creation(
          name = CommonUtils.requireNonEmpty(noJsNull(json)(NAME_KEY).convertTo[String]),
          imageName = CommonUtils.requireNonEmpty(
            noJsNull(json).get(IMAGE_NAME_KEY).map(_.convertTo[String]).getOrElse(IMAGE_NAME_DEFAULT)),
          brokerClusterName =
            noJsNull(json).get(BROKER_CLUSTER_NAME_KEY).map(_.convertTo[String]).map(CommonUtils.requireNonEmpty),
          clientPort = CommonUtils.requireConnectionPort(
            noJsNull(json).get(CLIENT_PORT_KEY).map(_.convertTo[Int]).getOrElse(CommonUtils.availablePort())),
          jmxPort = CommonUtils.requireConnectionPort(
            noJsNull(json).get(JMX_PORT_KEY).map(_.convertTo[Int]).getOrElse(CommonUtils.availablePort())),
          groupId = CommonUtils.requireNonEmpty(groupId),
          statusTopicName = CommonUtils.requireNonEmpty(
            noJsNull(json)
              .get(STATUS_TOPIC_NAME_KEY)
              .map(_.convertTo[String])
              .getOrElse(s"$groupId-status-${CommonUtils.randomString(10)}")),
          statusTopicPartitions = noJsNull(json).get(STATUS_TOPIC_PARTITIONS_KEY).map(_.convertTo[Int]).getOrElse(1),
          statusTopicReplications =
            noJsNull(json).get(STATUS_TOPIC_REPLICATIONS_KEY).map(_.convertTo[Short]).getOrElse(1),
          configTopicName = noJsNull(json)
            .get(CONFIG_TOPIC_NAME_KEY)
            .map(_.convertTo[String])
            .getOrElse(s"$groupId-config-${CommonUtils.randomString(10)}"),
          configTopicReplications =
            noJsNull(json).get(CONFIG_TOPIC_REPLICATIONS_KEY).map(_.convertTo[Short]).getOrElse(1),
          offsetTopicName = noJsNull(json)
            .get(OFFSET_TOPIC_NAME_KEY)
            .map(_.convertTo[String])
            .getOrElse(s"$groupId-offset-${CommonUtils.randomString(10)}"),
          offsetTopicPartitions = noJsNull(json).get(OFFSET_TOPIC_PARTITIONS_KEY).map(_.convertTo[Int]).getOrElse(1),
          offsetTopicReplications =
            noJsNull(json).get(OFFSET_TOPIC_REPLICATIONS_KEY).map(_.convertTo[Short]).getOrElse(1),
          jarIds = noJsNull(json)
            .get(JAR_IDS_KEY)
            .map(_.convertTo[Set[String]])
            .getOrElse(
              // TODO: remove this stale key
              json.asJsObject.fields.get(JARS_KEY).map(_.convertTo[Set[String]]).getOrElse(Set.empty)),
          nodeNames = noJsNull(json)(NODE_NAMES_KEY).convertTo[Seq[String]].toSet
        )
      } catch {
        case e: Throwable =>
          throw DeserializationException(e.getMessage, e)
      }
    }

  /**
    * exposed to configurator
    */
  private[ohara] implicit val SETTING_DEFINITION_JSON_FORMAT: RootJsonFormat[SettingDefinition] =
    new RootJsonFormat[SettingDefinition] {
      import spray.json._
      override def read(json: JsValue): SettingDefinition = SettingDefinition.ofJson(json.toString())

      override def write(obj: SettingDefinition): JsValue = obj.toJsonString.parseJson
    }

  final case class ConnectorDefinitions private[WorkerApi] (className: String, definitions: Seq[SettingDefinition])
  private[this] implicit val CONNECTION_DEFINITIONS_JSON_FORMAT: RootJsonFormat[ConnectorDefinitions] = jsonFormat2(
    ConnectorDefinitions)

  final case class WorkerClusterInfo private[ohara] (name: String,
                                                     imageName: String,
                                                     brokerClusterName: String,
                                                     clientPort: Int,
                                                     jmxPort: Int,
                                                     groupId: String,
                                                     statusTopicName: String,
                                                     statusTopicPartitions: Int,
                                                     statusTopicReplications: Short,
                                                     configTopicName: String,
                                                     configTopicPartitions: Int,
                                                     configTopicReplications: Short,
                                                     offsetTopicName: String,
                                                     offsetTopicPartitions: Int,
                                                     offsetTopicReplications: Short,
                                                     jarInfos: Seq[JarInfo],
                                                     connectors: Seq[ConnectorDefinitions],
                                                     nodeNames: Set[String])
      extends ClusterInfo {

    /**
      * Our client to broker and worker accept the connection props:host:port,host2:port2
      */
    def connectionProps: String = nodeNames.map(n => s"$n:$clientPort").mkString(",")

    def sources: Seq[ConnectorVersion] = connectors.map(InfoApi.toConnectorVersion).filter(_.typeName == "source")
    def sinks: Seq[ConnectorVersion] = connectors.map(InfoApi.toConnectorVersion).filter(_.typeName == "sink")
    override def ports: Set[Int] = Set(clientPort, jmxPort)

    override def clone(newNodeNames: Set[String]): ClusterInfo = copy(nodeNames = newNodeNames)
  }

  /**
    * exposed to configurator
    */
  private[ohara] implicit val WORKER_CLUSTER_INFO_JSON_FORMAT: RootJsonFormat[WorkerClusterInfo] =
    new RootJsonFormat[WorkerClusterInfo] {
      override def write(obj: WorkerClusterInfo): JsValue = JsObject(
        noJsNull(
          Map(
            NAME_KEY -> JsString(obj.name),
            IMAGE_NAME_KEY -> JsString(obj.imageName),
            BROKER_CLUSTER_NAME_KEY -> JsString(obj.brokerClusterName),
            CLIENT_PORT_KEY -> JsNumber(obj.clientPort),
            JMX_PORT_KEY -> JsNumber(obj.jmxPort),
            GROUP_ID_KEY -> JsString(obj.groupId),
            STATUS_TOPIC_NAME_KEY -> JsString(obj.statusTopicName),
            STATUS_TOPIC_PARTITIONS_KEY -> JsNumber(obj.statusTopicPartitions),
            STATUS_TOPIC_REPLICATIONS_KEY -> JsNumber(obj.statusTopicReplications),
            CONFIG_TOPIC_NAME_KEY -> JsString(obj.configTopicName),
            CONFIG_TOPIC_PARTITIONS_KEY -> JsNumber(obj.configTopicPartitions),
            CONFIG_TOPIC_REPLICATIONS_KEY -> JsNumber(obj.configTopicReplications),
            OFFSET_TOPIC_NAME_KEY -> JsString(obj.offsetTopicName),
            OFFSET_TOPIC_PARTITIONS_KEY -> JsNumber(obj.offsetTopicPartitions),
            OFFSET_TOPIC_REPLICATIONS_KEY -> JsNumber(obj.offsetTopicReplications),
            JAR_INFOS_KEY -> JsArray(obj.jarInfos.map(JAR_INFO_JSON_FORMAT.write).toVector),
            CONNECTORS_KEY -> JsArray(obj.connectors.map(CONNECTION_DEFINITIONS_JSON_FORMAT.write).toVector),
            NODE_NAMES_KEY -> JsArray(obj.nodeNames.map(JsString(_)).toVector),
            // deprecated keys
            JAR_NAMES_KEY -> JsArray(obj.jarInfos.map(_.id).map(JsString(_)).toVector),
            // deprecated keys
            SOURCES_KEY -> JsArray(
              obj.connectors
                .map(InfoApi.toConnectorVersion)
                .filter(_.typeName == "source")
                .map(InfoApi.CONNECTOR_VERSION_JSON_FORMAT.write)
                .toVector),
            // deprecated keys
            SINKS_KEY -> JsArray(
              obj.connectors
                .map(InfoApi.toConnectorVersion)
                .filter(_.typeName == "sink")
                .map(InfoApi.CONNECTOR_VERSION_JSON_FORMAT.write)
                .toVector)
          ))
      )

      override def read(json: JsValue): WorkerClusterInfo = WorkerClusterInfo(
        name = noJsNull(json)(NAME_KEY).convertTo[String],
        imageName = noJsNull(json)(IMAGE_NAME_KEY).convertTo[String],
        brokerClusterName = noJsNull(json)(BROKER_CLUSTER_NAME_KEY).convertTo[String],
        clientPort = noJsNull(json)(CLIENT_PORT_KEY).convertTo[Int],
        jmxPort = noJsNull(json)(JMX_PORT_KEY).convertTo[Int],
        groupId = noJsNull(json)(GROUP_ID_KEY).convertTo[String],
        statusTopicName = noJsNull(json)(STATUS_TOPIC_NAME_KEY).convertTo[String],
        statusTopicPartitions = noJsNull(json)(STATUS_TOPIC_PARTITIONS_KEY).convertTo[Int],
        statusTopicReplications = noJsNull(json)(STATUS_TOPIC_REPLICATIONS_KEY).convertTo[Short],
        configTopicName = noJsNull(json)(CONFIG_TOPIC_NAME_KEY).convertTo[String],
        configTopicPartitions = noJsNull(json)(CONFIG_TOPIC_PARTITIONS_KEY).convertTo[Int],
        configTopicReplications = noJsNull(json)(CONFIG_TOPIC_REPLICATIONS_KEY).convertTo[Short],
        offsetTopicName = noJsNull(json)(OFFSET_TOPIC_NAME_KEY).convertTo[String],
        offsetTopicPartitions = noJsNull(json)(OFFSET_TOPIC_PARTITIONS_KEY).convertTo[Int],
        offsetTopicReplications = noJsNull(json)(OFFSET_TOPIC_REPLICATIONS_KEY).convertTo[Short],
        jarInfos = noJsNull(json)(JAR_INFOS_KEY).convertTo[Seq[JarInfo]],
        connectors = noJsNull(json)(CONNECTORS_KEY).convertTo[Seq[ConnectorDefinitions]],
        nodeNames = noJsNull(json)(NODE_NAMES_KEY).convertTo[Seq[String]].toSet
      )
    }

  /**
    * used to generate the payload and url for POST/PUT request.
    */
  sealed trait Request {
    def name(name: String): Request
    @Optional("the default image is IMAGE_NAME_DEFAULT")
    def imageName(imageName: String): Request
    @Optional("the default port is random")
    def clientPort(clientPort: Int): Request
    @Optional("the default port is random")
    def jmxPort(jmxPort: Int): Request
    @Optional("Ignoring the name will invoke an auto-mapping to existent broker cluster")
    def brokerClusterName(brokerClusterName: String): Request
    @Optional("the default port is random")
    def groupId(groupId: String): Request
    @Optional("the default port is random")
    def statusTopicName(statusTopicName: String): Request
    @Optional("the default number is 1")
    def statusTopicPartitions(statusTopicPartitions: Int): Request
    @Optional("the default number is 1")
    def statusTopicReplications(statusTopicReplications: Short): Request
    @Optional("the default number is random")
    def configTopicName(configTopicName: String): Request
    @Optional("the default number is 1")
    def configTopicReplications(configTopicReplications: Short): Request
    def offsetTopicName(offsetTopicName: String): Request
    @Optional("the default number is 1")
    def offsetTopicPartitions(offsetTopicPartitions: Int): Request
    @Optional("the default number is 1")
    def offsetTopicReplications(offsetTopicReplications: Short): Request
    @Optional("the default value is empty")
    def jarIds(jarIds: Set[String]): Request
    def nodeName(nodeName: String): Request = nodeNames(Set(CommonUtils.requireNonEmpty(nodeName)))
    def nodeNames(nodeNames: Set[String]): Request

    /**
      * generate the POST request
      * @param executionContext thread pool
      * @return created data
      */
    def create()(implicit executionContext: ExecutionContext): Future[WorkerClusterInfo]

    /**
      * @return the payload of creation
      */
    private[v0] def creation(): Creation
  }

  final class Access private[WorkerApi] extends ClusterAccess[WorkerClusterInfo](WORKER_PREFIX_PATH) {
    def request(): Request = new Request {
      private[this] var name: String = _
      private[this] var imageName: String = IMAGE_NAME_DEFAULT
      private[this] var brokerClusterName: String = _
      private[this] var clientPort: Int = CommonUtils.availablePort()
      private[this] var jmxPort: Int = CommonUtils.availablePort()
      private[this] var groupId: String = CommonUtils.randomString(10)
      private[this] var configTopicName: String = s"$groupId-config-${CommonUtils.randomString(10)}"
      private[this] var configTopicReplications: Short = 1
      private[this] var offsetTopicName: String = s"$groupId-offset-${CommonUtils.randomString(10)}"
      private[this] var offsetTopicPartitions: Int = 1
      private[this] var offsetTopicReplications: Short = 1
      private[this] var statusTopicName: String = s"$groupId-status-${CommonUtils.randomString(10)}"
      private[this] var statusTopicPartitions: Int = 1
      private[this] var statusTopicReplications: Short = 1
      private[this] var jarIds: Set[String] = Set.empty
      private[this] var nodeNames: Set[String] = Set.empty

      private[this] def legalNumber(number: Int, key: String): Int = {
        if (number <= 0) throw new IllegalArgumentException(s"the number of $key must be bigger than zero")
        number
      }

      private[this] def legalNumber(number: Short, key: String): Short = {
        if (number <= 0) throw new IllegalArgumentException(s"the number of $key must be bigger than zero")
        number
      }

      override def name(name: String): Request = {
        this.name = CommonUtils.requireNonEmpty(name)
        this
      }

      override def imageName(imageName: String): Request = {
        this.imageName = CommonUtils.requireNonEmpty(imageName)
        this
      }

      override def clientPort(clientPort: Int): Request = {
        this.clientPort = CommonUtils.requireConnectionPort(clientPort)
        this
      }

      override def jmxPort(jmxPort: Int): Request = {
        this.jmxPort = CommonUtils.requireConnectionPort(jmxPort)
        this
      }

      override def brokerClusterName(brokerClusterName: String): Request = {
        this.brokerClusterName = CommonUtils.requireNonEmpty(brokerClusterName)
        this
      }

      override def groupId(groupId: String): Request = {
        this.groupId = CommonUtils.requireNonEmpty(groupId)
        this
      }

      override def statusTopicName(statusTopicName: String): Request = {
        this.statusTopicName = CommonUtils.requireNonEmpty(statusTopicName)
        this
      }

      override def statusTopicPartitions(statusTopicPartitions: Int): Request = {
        this.statusTopicPartitions = legalNumber(statusTopicPartitions, "statusTopicPartitions")
        this
      }

      override def statusTopicReplications(statusTopicReplications: Short): Request = {
        this.statusTopicReplications = legalNumber(statusTopicReplications, "statusTopicReplications")
        this
      }

      override def configTopicName(configTopicName: String): Request = {
        this.configTopicName = CommonUtils.requireNonEmpty(configTopicName)
        this
      }

      override def configTopicReplications(configTopicReplications: Short): Request = {
        this.configTopicReplications = legalNumber(configTopicReplications, "configTopicReplications")
        this
      }

      override def offsetTopicName(offsetTopicName: String): Request = {
        this.offsetTopicName = CommonUtils.requireNonEmpty(offsetTopicName)
        this
      }

      override def offsetTopicPartitions(offsetTopicPartitions: Int): Request = {
        this.offsetTopicPartitions = legalNumber(offsetTopicPartitions, "offsetTopicPartitions")
        this
      }

      override def offsetTopicReplications(offsetTopicReplications: Short): Request = {
        this.offsetTopicReplications = legalNumber(offsetTopicReplications, "offsetTopicReplications")
        this
      }

      import scala.collection.JavaConverters._
      override def jarIds(jarIds: Set[String]): Request = {
        this.jarIds = CommonUtils.requireNonEmpty(jarIds.asJava).asScala.toSet
        this
      }

      override def nodeNames(nodeNames: Set[String]): Request = {
        this.nodeNames = CommonUtils.requireNonEmpty(nodeNames.asJava).asScala.toSet
        this
      }

      override private[v0] def creation() = Creation(
        name = CommonUtils.requireNonEmpty(name),
        imageName = CommonUtils.requireNonEmpty(imageName),
        brokerClusterName = Option(brokerClusterName),
        clientPort = CommonUtils.requireConnectionPort(clientPort),
        jmxPort = CommonUtils.requireConnectionPort(jmxPort),
        groupId = CommonUtils.requireNonEmpty(groupId),
        configTopicName = CommonUtils.requireNonEmpty(configTopicName),
        configTopicReplications = legalNumber(configTopicReplications, "configTopicReplications"),
        offsetTopicName = CommonUtils.requireNonEmpty(offsetTopicName),
        offsetTopicPartitions = legalNumber(offsetTopicPartitions, "offsetTopicPartitions"),
        offsetTopicReplications = legalNumber(offsetTopicReplications, "offsetTopicReplications"),
        statusTopicName = CommonUtils.requireNonEmpty(statusTopicName),
        statusTopicPartitions = legalNumber(statusTopicPartitions, "statusTopicPartitions"),
        statusTopicReplications = legalNumber(statusTopicReplications, "statusTopicReplications"),
        jarIds = Objects.requireNonNull(jarIds),
        nodeNames = CommonUtils.requireNonEmpty(nodeNames.asJava).asScala.toSet
      )

      override def create()(implicit executionContext: ExecutionContext): Future[WorkerClusterInfo] =
        exec.post[Creation, WorkerClusterInfo, ErrorApi.Error](
          _url,
          creation()
        )
    }
  }

  def access(): Access = new Access
}
