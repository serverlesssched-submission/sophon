/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.containerpool.kubernetes

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.event.Logging.InfoLevel
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.apache.openwhisk.common.{Logging, LoggingMarkers, TransactionId}
import org.apache.openwhisk.core.containerpool._
import org.apache.openwhisk.core.entity.size.SizeInt
import org.apache.openwhisk.core.entity.{ActivationResponse, ByteSize}
import spray.json.JsObject

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.concurrent.duration._
import scala.language.postfixOps

object KubernetesContainer {

  var concurrentCreate = new AtomicInteger(0)
  var lastFinish: Instant = Instant.EPOCH
//  def serviceLatency = 1150
  def serviceLatency = 1650
  def serviceRate = 8
  def create(transid: TransactionId,
             name: String,
             image: String,
             userProvidedImage: Boolean = false,
             memory: ByteSize = 256.MB,
             environment: Map[String, String] = Map.empty,
             labels: Map[String, String] = Map.empty)(implicit as: ActorSystem,
                                                      ec: ExecutionContext,
                                                      logging: Logging): Future[KubernetesContainer] = {
    val containerName = name.take(63)
    val start = transid.started(
      this,
      LoggingMarkers.INVOKER_KUBEAPI_CMD("create"),
      s"${System.currentTimeMillis} launching pod $containerName (image:$image, mem: ${memory.toMB})",
      logLevel = akka.event.Logging.InfoLevel)

    val p = Promise[KubernetesContainer]()

    var totalLatency = 0L
    KubernetesContainer.synchronized {
      val expected = Instant.now.plusMillis(serviceLatency)
      val queued = lastFinish.plusMillis(1 / serviceRate * 1000)
      val finish = if (expected.isAfter(queued)) expected else queued
      lastFinish = finish
      totalLatency = finish.toEpochMilli - Instant.now.toEpochMilli
    }

    as.scheduler.scheduleOnce(totalLatency millisecond){
      transid.finished(this, start, logLevel = InfoLevel)
      p.success(new KubernetesContainer(ContainerId(containerName), ContainerAddress(containerName))(as, ec, logging))
    }(ec)
    p.future
  }

}

/**
 * @constructor
 * @param id the id of the container
 */
class KubernetesContainer(protected[core] val id: ContainerId,
                    protected[core] val addr: ContainerAddress) (override implicit val as: ActorSystem,
                    protected implicit val ec: ExecutionContext,
                    protected val logging: Logging) extends Container {

  def removeLatency: Int = 7

  override def suspend()(implicit transid: TransactionId): Future[Unit] = Future.successful({})

  override def resume()(implicit transid: TransactionId): Future[Unit] = Future.successful({})

  override def destroy()(implicit transid: TransactionId): Future[Unit] = {
    val start = transid.started(
      this,
      LoggingMarkers.INVOKER_KUBEAPI_CMD("delete"),
      s"${System.currentTimeMillis} Deleting pod ${id.asString}",
      logLevel = akka.event.Logging.InfoLevel)(logging)
    Future {
      blocking(Thread.sleep(removeLatency))
      transid.finished(this, start, logLevel = InfoLevel)(logging)
      Future.successful({})
    }
  }

  override def logs(limit: ByteSize, waitForSentinel: Boolean)(implicit transid: TransactionId): Source[ByteString, Any] = Source.empty

  override def initialize(initializer: JsObject, timeout: FiniteDuration, maxConcurrent: Int)(
    implicit transid: TransactionId): Future[Interval] = {
    def initLatency: Int = 350
    Future {
      val start = Instant.now()
      blocking(Thread.sleep(initLatency))
      Interval(start, Instant.now())
    }
  }

  override def run(parameters: JsObject,
                   environment: JsObject,
                   timeout: FiniteDuration,
                   maxConcurrent: Int,
                   reschedule: Boolean = false,
                   actionName: String)(implicit transid: TransactionId): Future[(Interval, ActivationResponse)] = {
    Future {
      val start = Instant.now()
      parameters.fields.get("duration") match {
        case Some(value) =>
          logging.info(this, s"${System.currentTimeMillis} sending arguments to ${actionName} at ${id}")(transid)
          blocking(Thread.sleep((value.toString.toFloat*1000).toInt))
          logging.info(this, s"${System.currentTimeMillis} running results ")(transid)
          (Interval(start, Instant.now()), ActivationResponse.success(Some(parameters)))
        case None if actionName.startsWith("invokerHealthTestAction") =>
          logging.info(this, s"invokerHealthTestAction")(transid)
          (Interval(start, Instant.now()), ActivationResponse.success(Some(parameters)))
        case _ =>
          logging.warn(this, s"duration parameter nonexistent: ${parameters.toString}")(transid)
          (Interval.zero, ActivationResponse.developerError("duration parameter nonexistent"))
      }
    }
  }
}
