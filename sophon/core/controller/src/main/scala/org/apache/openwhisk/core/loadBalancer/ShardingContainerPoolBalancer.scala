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

package org.apache.openwhisk.core.loadBalancer

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

import akka.actor.ActorRef
import akka.actor.ActorRefFactory
import akka.actor.{Actor, ActorSystem, Cancellable, Props}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.management.scaladsl.AkkaManagement
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.stream.ActorMaterializer
import org.apache.kafka.clients.producer.RecordMetadata
import pureconfig._
import pureconfig.generic.auto._
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.WhiskConfig._
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size.SizeLong
import org.apache.openwhisk.common.LoggingMarkers._
import org.apache.openwhisk.core.loadBalancer.InvokerState.{Healthy, Offline, Unhealthy, Unresponsive}
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}
import org.apache.openwhisk.spi.SpiLoader

import scala.Float.PositiveInfinity
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.FiniteDuration
import scala.math.exp

// activation queue msg
case class PublishToQueue(action: ExecutableWhiskActionMetaData, msg: ActivationMessage, promise: Promise[Future[Either[ActivationId, WhiskActivation]]], tid: TransactionId)
case class ProcessQueue()

/**
 * A loadbalancer that schedules workload based on a hashing-algorithm.
 *
 * ## Algorithm
 *
 * At first, for every namespace + action pair a hash is calculated and then an invoker is picked based on that hash
 * (`hash % numInvokers`). The determined index is the so called "home-invoker". This is the invoker where the following
 * progression will **always** start. If this invoker is healthy (see "Invoker health checking") and if there is
 * capacity on that invoker (see "Capacity checking"), the request is scheduled to it.
 *
 * If one of these prerequisites is not true, the index is incremented by a step-size. The step-sizes available are the
 * all coprime numbers smaller than the amount of invokers available (coprime, to minimize collisions while progressing
 * through the invokers). The step-size is picked by the same hash calculated above (`hash & numStepSizes`). The
 * home-invoker-index is now incremented by the step-size and the checks (healthy + capacity) are done on the invoker
 * we land on now.
 *
 * This procedure is repeated until all invokers have been checked at which point the "overload" strategy will be
 * employed, which is to choose a healthy invoker randomly. In a steadily running system, that overload means that there
 * is no capacity on any invoker left to schedule the current request to.
 *
 * If no invokers are available or if there are no healthy invokers in the system, the loadbalancer will return an error
 * stating that no invokers are available to take any work. Requests are not queued anywhere in this case.
 *
 * An example:
 * - availableInvokers: 10 (all healthy)
 * - hash: 13
 * - homeInvoker: hash % availableInvokers = 13 % 10 = 3
 * - stepSizes: 1, 3, 7 (note how 2 and 5 is not part of this because it's not coprime to 10)
 * - stepSizeIndex: hash % numStepSizes = 13 % 3 = 1 => stepSize = 3
 *
 * Progression to check the invokers: 3, 6, 9, 2, 5, 8, 1, 4, 7, 0 --> done
 *
 * This heuristic is based on the assumption, that the chance to get a warm container is the best on the home invoker
 * and degrades the more steps you make. The hashing makes sure that all loadbalancers in a cluster will always pick the
 * same home invoker and do the same progression for a given action.
 *
 * Known caveats:
 * - This assumption is not always true. For instance, two heavy workloads landing on the same invoker can override each
 *   other, which results in many cold starts due to all containers being evicted by the invoker to make space for the
 *   "other" workload respectively. Future work could be to keep a buffer of invokers last scheduled for each action and
 *   to prefer to pick that one. Then the second-last one and so forth.
 *
 * ## Capacity checking
 *
 * The maximum capacity per invoker is configured using `user-memory`, which is the maximum amount of memory of actions
 * running in parallel on that invoker.
 *
 * Spare capacity is determined by what the loadbalancer thinks it scheduled to each invoker. Upon scheduling, an entry
 * is made to update the books and a slot for each MB of the actions memory limit in a Semaphore is taken. These slots
 * are only released after the response from the invoker (active-ack) arrives **or** after the active-ack times out.
 * The Semaphore has as many slots as MBs are configured in `user-memory`.
 *
 * Known caveats:
 * - In an overload scenario, activations are queued directly to the invokers, which makes the active-ack timeout
 *   unpredictable. Timing out active-acks in that case can cause the loadbalancer to prematurely assign new load to an
 *   overloaded invoker, which can cause uneven queues.
 * - The same is true if an invoker is extraordinarily slow in processing activations. The queue on this invoker will
 *   slowly rise if it gets slow to the point of still sending pings, but handling the load so slowly, that the
 *   active-acks time out. The loadbalancer again will think there is capacity, when there is none.
 *
 * Both caveats could be solved in future work by not queueing to invoker topics on overload, but to queue on a
 * centralized overflow topic. Timing out an active-ack can then be seen as a system-error, as described in the
 * following.
 *
 * ## Invoker health checking
 *
 * Invoker health is determined via a kafka-based protocol, where each invoker pings the loadbalancer every second. If
 * no ping is seen for a defined amount of time, the invoker is considered "Offline".
 *
 * Moreover, results from all activations are inspected. If more than 3 out of the last 10 activations contained system
 * errors, the invoker is considered "Unhealthy". If an invoker is unhealty, no user workload is sent to it, but
 * test-actions are sent by the loadbalancer to check if system errors are still happening. If the
 * system-error-threshold-count in the last 10 activations falls below 3, the invoker is considered "Healthy" again.
 *
 * To summarize:
 * - "Offline": Ping missing for > 10 seconds
 * - "Unhealthy": > 3 **system-errors** in the last 10 activations, pings arriving as usual
 * - "Healthy": < 3 **system-errors** in the last 10 activations, pings arriving as usual
 *
 * ## Horizontal sharding
 *
 * Sharding is employed to avoid both loadbalancers having to share any data, because the metrics used in scheduling
 * are very fast changing.
 *
 * Horizontal sharding means, that each invoker's capacity is evenly divided between the loadbalancers. If an invoker
 * has at most 16 slots available (invoker-busy-threshold = 16), those will be divided to 8 slots for each loadbalancer
 * (if there are 2).
 *
 * If concurrent activation processing is enabled (and concurrency limit is > 1), accounting of containers and
 * concurrency capacity per container will limit the number of concurrent activations routed to the particular
 * slot at an invoker. Default max concurrency is 1.
 *
 * Known caveats:
 * - If a loadbalancer leaves or joins the cluster, all state is removed and created from scratch. Those events should
 *   not happen often.
 * - If concurrent activation processing is enabled, it only accounts for the containers that the current loadbalancer knows.
 *   So the actual number of containers launched at the invoker may be less than is counted at the loadbalancer, since
 *   the invoker may skip container launch in case there is concurrent capacity available for a container launched via
 *   some other loadbalancer.
 */
class ShardingContainerPoolBalancer(
  config: WhiskConfig,
  controllerInstance: ControllerInstanceId,
  feedFactory: FeedFactory,
  val invokerPoolFactory: InvokerPoolFactory,
  implicit val messagingProvider: MessagingProvider = SpiLoader.get[MessagingProvider])(
  implicit actorSystem: ActorSystem,
  logging: Logging,
  materializer: ActorMaterializer)
    extends CommonLoadBalancer(config, feedFactory, controllerInstance) {

  /** Build a cluster of all loadbalancers */
  private val cluster: Option[Cluster] = if (loadConfigOrThrow[ClusterConfig](ConfigKeys.cluster).useClusterBootstrap) {
    AkkaManagement(actorSystem).start()
    ClusterBootstrap(actorSystem).start()
    Some(Cluster(actorSystem))
  } else if (loadConfigOrThrow[Seq[String]]("akka.cluster.seed-nodes").nonEmpty) {
    Some(Cluster(actorSystem))
  } else {
    None
  }

  override protected def emitStats(): Unit = {
    val available = for (invoker <- schedulingState.invokerSlots) yield invoker.availablePermits
    val free = for (invoker <- schedulingState.invokerSlots) yield invoker.freePermits
    logging.info(this, s"${System.currentTimeMillis} emitting available $available free $free")
  }

  override protected def emitMetrics() = {
    super.emitMetrics()
    MetricEmitter.emitGaugeMetric(
      INVOKER_TOTALMEM_BLACKBOX,
      schedulingState.blackboxInvokers.foldLeft(0L) { (total, curr) =>
        if (curr.status.isUsable) {
          curr.id.userMemory.toMB + total
        } else {
          total
        }
      })
    MetricEmitter.emitGaugeMetric(
      INVOKER_TOTALMEM_MANAGED,
      schedulingState.managedInvokers.foldLeft(0L) { (total, curr) =>
        if (curr.status.isUsable) {
          curr.id.userMemory.toMB + total
        } else {
          total
        }
      })
    MetricEmitter.emitGaugeMetric(HEALTHY_INVOKER_MANAGED, schedulingState.managedInvokers.count(_.status == Healthy))
    MetricEmitter.emitGaugeMetric(
      UNHEALTHY_INVOKER_MANAGED,
      schedulingState.managedInvokers.count(_.status == Unhealthy))
    MetricEmitter.emitGaugeMetric(
      UNRESPONSIVE_INVOKER_MANAGED,
      schedulingState.managedInvokers.count(_.status == Unresponsive))
    MetricEmitter.emitGaugeMetric(OFFLINE_INVOKER_MANAGED, schedulingState.managedInvokers.count(_.status == Offline))
    MetricEmitter.emitGaugeMetric(HEALTHY_INVOKER_BLACKBOX, schedulingState.blackboxInvokers.count(_.status == Healthy))
    MetricEmitter.emitGaugeMetric(
      UNHEALTHY_INVOKER_BLACKBOX,
      schedulingState.blackboxInvokers.count(_.status == Unhealthy))
    MetricEmitter.emitGaugeMetric(
      UNRESPONSIVE_INVOKER_BLACKBOX,
      schedulingState.blackboxInvokers.count(_.status == Unresponsive))
    MetricEmitter.emitGaugeMetric(OFFLINE_INVOKER_BLACKBOX, schedulingState.blackboxInvokers.count(_.status == Offline))
  }

  /** State needed for scheduling. */
  val schedulingState = ShardingContainerPoolBalancerState()(lbConfig)

  /**
   * Monitors invoker supervision and the cluster to update the state sequentially
   *
   * All state updates should go through this actor to guarantee that
   * [[ShardingContainerPoolBalancerState.updateInvokers]] and [[ShardingContainerPoolBalancerState.updateCluster]]
   * are called exclusive of each other and not concurrently.
   */
  private val monitor = actorSystem.actorOf(Props(new Actor {
    override def preStart(): Unit = {
      cluster.foreach(_.subscribe(self, classOf[MemberEvent], classOf[ReachabilityEvent]))
    }

    // all members of the cluster that are available
    var availableMembers = Set.empty[Member]

    override def receive: Receive = {
      case CurrentInvokerPoolState(newState) =>
        schedulingState.updateInvokers(newState)

      // State of the cluster as it is right now
      case CurrentClusterState(members, _, _, _, _) =>
        availableMembers = members.filter(_.status == MemberStatus.Up)
        schedulingState.updateCluster(availableMembers.size)

      // General lifecycle events and events concerning the reachability of members. Split-brain is not a huge concern
      // in this case as only the invoker-threshold is adjusted according to the perceived cluster-size.
      // Taking the unreachable member out of the cluster from that point-of-view results in a better experience
      // even under split-brain-conditions, as that (in the worst-case) results in premature overloading of invokers vs.
      // going into overflow mode prematurely.
      case event: ClusterDomainEvent =>
        availableMembers = event match {
          case MemberUp(member)          => availableMembers + member
          case ReachableMember(member)   => availableMembers + member
          case MemberRemoved(member, _)  => availableMembers - member
          case UnreachableMember(member) => availableMembers - member
          case _                         => availableMembers
        }

        schedulingState.updateCluster(availableMembers.size)
    }
  }))

  /** Loadbalancer interface methods */
  override def invokerHealth(): Future[IndexedSeq[InvokerHealth]] = Future.successful(schedulingState.invokers)
  override def clusterSize: Int = schedulingState.clusterSize

//  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(
//    implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {
//    val postPromise = Promise[Future[Either[ActivationId, WhiskActivation]]]()
//    queueActor ! PublishToQueue(action, msg, postPromise, transid)
//    postPromise.future
//  }

  /** 1. Publish a message to the loadbalancer */
  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(
    implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {

    val isBlackboxInvocation = action.exec.pull
    val actionType = if (!isBlackboxInvocation) "managed" else "blackbox"
    val (invokersToUse, stepSizes) =
      if (!isBlackboxInvocation) (schedulingState.managedInvokers, schedulingState.managedStepSizes)
      else (schedulingState.blackboxInvokers, schedulingState.blackboxStepSizes)
    val chosen = if (invokersToUse.nonEmpty) {
      val hash = ShardingContainerPoolBalancer.generateHash(msg.user.namespace.name, action.fullyQualifiedName(false))
      val homeInvoker = hash % invokersToUse.size
      val stepSize = stepSizes(hash % stepSizes.size)
      val invoker: Option[(InvokerInstanceId, Boolean, String)] = ShardingContainerPoolBalancer.schedule(
        action.limits.concurrency.maxConcurrent,
        action.fullyQualifiedName(true),
        invokersToUse,
        schedulingState.invokerSlots,
        action.limits.memory.megabytes,
        homeInvoker,
        stepSize,
        0,
        lbConfig,
        schedulingState)
      invoker.foreach {
        case (_, true, _) =>
          val metric =
            if (isBlackboxInvocation)
              LoggingMarkers.BLACKBOX_SYSTEM_OVERLOAD
            else
              LoggingMarkers.MANAGED_SYSTEM_OVERLOAD
          MetricEmitter.emitCounterMetric(metric)
        case _ =>
      }
      invoker
    } else {
      None
    }

    chosen
      .map { invoker =>
        // MemoryLimit() and TimeLimit() return singletons - they should be fast enough to be used here
        msg.container = invoker._3
        val memoryLimit = action.limits.memory
        val memoryLimitInfo = if (memoryLimit == MemoryLimit()) { "std" } else { "non-std" }
        val timeLimit = action.limits.timeout
        val timeLimitInfo = if (timeLimit == TimeLimit()) { "std" } else { "non-std" }
        val timestamp: Long = System.currentTimeMillis
        logging.info(
          this,
          s"$timestamp scheduled activation ${msg.activationId}, action '${msg.action.asString}' ($actionType), ns '${msg.user.namespace.name.asString}', mem limit ${memoryLimit.megabytes} MB (${memoryLimitInfo}), time limit ${timeLimit.duration.toMillis} ms (${timeLimitInfo}) to ${invoker}")
        val activationResult = setupActivation(msg, action, invoker._1, invoker._3)
        sendActivationToInvoker(messageProducer, msg, invoker._1).map(_ => activationResult)
      }
      .getOrElse {
        // report the state of all invokers
        val invokerStates = invokersToUse.foldLeft(Map.empty[InvokerState, Int]) { (agg, curr) =>
          val count = agg.getOrElse(curr.status, 0) + 1
          agg + (curr.status -> count)
        }

        logging.error(
          this,
          s"failed to schedule activation ${msg.activationId}, action '${msg.action.asString}' ($actionType), ns '${msg.user.namespace.name.asString}'")
        Future.failed(LoadBalancerException("No invokers available"))
      }
  }

  override val invokerPool =
    invokerPoolFactory.createInvokerPool(
      actorSystem,
      messagingProvider,
      messageProducer,
      sendActivationToInvoker,
      Some(monitor))

  override protected def releaseInvoker(invoker: InvokerInstanceId, entry: ActivationEntry, schedContainer: String, usedContainer: String, lastUsed: Instant): Unit = {
    val lambda =
      if (entry.fullyQualifiedEntityName.name.toString.startsWith("burst_parallel")) {
        logging.info(this, s"releaseInvoker w/ burst_parallel container: ${usedContainer}")
        lbConfig.bpLambda }
      else {
        logging.info(this, s"releaseInvoker w/ normal container: ${usedContainer}")
        lbConfig.lambda }
    schedulingState.invokerSlots
      .lift(invoker.toInt)
      .foreach(_.releaseConcurrent(entry.fullyQualifiedEntityName, entry.maxConcurrent, entry.memoryLimit.toMB.toInt, schedContainer, usedContainer, lastUsed, lambda.toFloat))
//    queueActor ! ProcessQueue // process queue when resources change
  }

  override protected def removeContainer(invoker: InvokerInstanceId, container: String): Unit = {
    schedulingState.invokerSlots
      .lift(invoker.toInt)
      .foreach(_.removeContainer(container))
  }
}

object ShardingContainerPoolBalancer extends LoadBalancerProvider {

  override def instance(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(
    implicit actorSystem: ActorSystem,
    logging: Logging,
    materializer: ActorMaterializer): LoadBalancer = {

    val invokerPoolFactory = new InvokerPoolFactory {
      override def createInvokerPool(
        actorRefFactory: ActorRefFactory,
        messagingProvider: MessagingProvider,
        messagingProducer: MessageProducer,
        sendActivationToInvoker: (MessageProducer, ActivationMessage, InvokerInstanceId) => Future[RecordMetadata],
        monitor: Option[ActorRef]): ActorRef = {

        InvokerPool.prepare(instance, WhiskEntityStore.datastore())

        actorRefFactory.actorOf(
          InvokerPool.props(
            (f, i) => f.actorOf(InvokerActor.props(i, instance)),
            (m, i) => sendActivationToInvoker(messagingProducer, m, i),
            messagingProvider.getConsumer(whiskConfig, s"health${instance.asString}", "health", maxPeek = 128),
            monitor))
      }

    }
    new ShardingContainerPoolBalancer(
      whiskConfig,
      instance,
      createFeedFactory(whiskConfig, instance),
      invokerPoolFactory)
  }

  def requiredProperties: Map[String, String] = kafkaHosts

  /** Generates a hash based on the string representation of namespace and action */
  def generateHash(namespace: EntityName, action: FullyQualifiedEntityName): Int = {
    (namespace.asString.hashCode() ^ action.asString.hashCode()).abs
  }

  /** Euclidean algorithm to determine the greatest-common-divisor */
  @tailrec
  def gcd(a: Int, b: Int): Int = if (b == 0) a else gcd(b, a % b)

  /** Returns pairwise coprime numbers until x. Result is memoized. */
  def pairwiseCoprimeNumbersUntil(x: Int): IndexedSeq[Int] =
    (1 to x).foldLeft(IndexedSeq.empty[Int])((primes, cur) => {
      if (gcd(cur, x) == 1 && primes.forall(i => gcd(i, cur) == 1)) {
        primes :+ cur
      } else primes
    })

  def schedule(maxConcurrent: Int,
               fqn: FullyQualifiedEntityName,
               invokers: IndexedSeq[InvokerHealth],
               dispatched: IndexedSeq[ContainerSemaphore],
               slots: Int,
               index: Int,
               step: Int,
               stepsDone: Int,
               lbConfig: ShardingContainerPoolBalancerConfig,
               schedulingState: ShardingContainerPoolBalancerState)(implicit logging: Logging, transId: TransactionId): Option[(InvokerInstanceId, Boolean, String)] = {
    val params = if (fqn.name.toString.startsWith("burst_parallel")) {
      (lbConfig.bpLambda, lbConfig.bpdistanceWeight, lbConfig.bpevictionWeight, lbConfig.bpacceptThreshold)
    } else {
      (lbConfig.lambda, lbConfig.distanceWeight, lbConfig.evictionWeight, lbConfig.acceptThreshold)
    }
    val res = lbConfig.schedulingPolicy match {
      case "vanilla" => vanillaPolicy(maxConcurrent, fqn, invokers, dispatched, slots, index, step, stepsDone, params)
      case "sophon" => sophonPolicy(maxConcurrent, fqn, invokers, dispatched, slots, index, step, stepsDone, None, PositiveInfinity, 0, params)
      case "sophon2" => sophon2Policy(maxConcurrent, fqn, invokers, dispatched, slots, index, step, stepsDone, None, PositiveInfinity, 0, params, lbConfig, schedulingState)
      case "max-available" => maxAvailablePolicy(maxConcurrent, fqn, invokers, dispatched, slots, index, step, stepsDone, None, 0, 0, params)
      case "max-free" => maxFreePolicy(maxConcurrent, fqn, invokers, dispatched, slots, index, step, stepsDone, None, 0, 0, params)
      case "sticky-available" => stickyAvailablePolicy(maxConcurrent, fqn, invokers, dispatched, slots, index, step, stepsDone, None, PositiveInfinity, 0, params)
      case "sticky-free" => stickyFreePolicy(maxConcurrent, fqn, invokers, dispatched, slots, index, step, stepsDone, None, PositiveInfinity, 0, params)
      case "random" =>
        val rand = scala.util.Random.nextInt(2147483647)
        val strides = schedulingState.managedStepSizes
        randomPolicy(maxConcurrent, fqn, invokers, dispatched, slots, rand%schedulingState.managedInvokers.size, strides(rand%strides.size), stepsDone, params)
      case _ =>
        logging.error(this, s"unknown scheduling policy ${lbConfig.schedulingPolicy}.")
        None
    }
    res match {
      case None =>
        logging.warn(this, s"${System.currentTimeMillis()} system overloaded, returning an error")
        None
      case Some(invoker) =>
        Some(invoker._1, invoker._2, dispatched(invoker._1.toInt).acquire(fqn, slots, invoker._3))
    }
  }

  /**
   * Scans through all invokers and searches for an invoker tries to get a free slot on an invoker. If no slot can be
   * obtained, randomly picks a healthy invoker.
   *
   * @param maxConcurrent concurrency limit supported by this action
   * @param invokers a list of available invokers to search in, including their state
   * @param dispatched semaphores for each invoker to give the slots away from
   * @param slots Number of slots, that need to be acquired (e.g. memory in MB)
   * @param index the index to start from (initially should be the "homeInvoker"
   * @param step stable identifier of the entity to be scheduled
   * @return an invoker to schedule to or None of no invoker is available
   */
  @tailrec
  def sophonPolicy(
    maxConcurrent: Int,
    fqn: FullyQualifiedEntityName,
    invokers: IndexedSeq[InvokerHealth],
    dispatched: IndexedSeq[ContainerSemaphore],
    slots: Int,
    index: Int,
    step: Int,
    stepsDone: Int, // use this as distance cost for spacial locality
    currentOptimal: Option[(InvokerInstanceId, Boolean, String)],
    minCost: Float,
    optimalDistance: Int,
    params: (Double,Double,Double,Double))(implicit logging: Logging, transId: TransactionId): Option[(InvokerInstanceId, Boolean, String)] = {
    // params: lbConfig.lambda, lbConfig.distanceWeight, lbConfig.evictionWeight, lbConfig.acceptThreshold
    val numInvokers = invokers.size
    logging.debug(this, s"${System.currentTimeMillis} chooseOptimal currentmin: ${minCost}")

    if (numInvokers > 0) {
      val invoker = invokers(index)
      val (ctnr, cost) = if (!invoker.status.isUsable) ("", PositiveInfinity) else dispatched (invoker.id.toInt).tryAcquire(fqn, maxConcurrent, slots, params)
      logging.debug(this, s"${System.currentTimeMillis} step ${stepsDone}, cost ${cost}")
      val newWeightedCost = cost*params._3 + stepsDone*params._2/numInvokers
      val newOptimal = if (newWeightedCost < minCost) Some(invoker.id, false, ctnr) else currentOptimal
      val newMinCost = if (newWeightedCost < minCost) newWeightedCost.toFloat else minCost
      val newDistance = if (newWeightedCost < minCost) stepsDone else optimalDistance
      if (stepsDone >= numInvokers || newMinCost <= (stepsDone+1)*params._2/numInvokers) { // traversed all invokers or current optimal cost must be lower than next invoker
        if (newMinCost <= params._4) {
          logging.info(this, s"${System.currentTimeMillis} chosen invoker ${newOptimal.get._1.toInt}, distance ${newDistance}, totalcost ${newMinCost}")
          newOptimal
        } else {
          logging.info(this, s"${System.currentTimeMillis} no invoker chosen, min cost ${newMinCost}")
          None }
      } else {
          val newIndex = (index + step) % numInvokers
          sophonPolicy(maxConcurrent, fqn, invokers, dispatched, slots, newIndex, step, stepsDone + 1, newOptimal, newMinCost, newDistance, params)
      }
    } else {
      None
    }
  }


  def printContainerMap(containerMap: ConcurrentHashMap[FullyQualifiedEntityName, mutable.Set[WarmContainer]], logging: Logging) : Unit = {}

  def sophon2Policy(maxConcurrent: Int,
                    fqn: FullyQualifiedEntityName,
                    invokers: IndexedSeq[InvokerHealth],
                    dispatched: IndexedSeq[ContainerSemaphore],
                    slots: Int,
                    index: Int,
                    step: Int,
                    stepsDone: Int, // use this as distance cost for spacial locality
                    currentOptimal: Option[(InvokerInstanceId, Boolean, String)],
                    minCost: Float,
                    optimalDistance: Int,
                    params: (Double,Double,Double,Double),
                    lbConfig: ShardingContainerPoolBalancerConfig,
                    schedulingState: ShardingContainerPoolBalancerState)(implicit logging: Logging, transId: TransactionId): Option[(InvokerInstanceId, Boolean, String)] = {
    val warmContainers = schedulingState.containerMap.getOrDefault(fqn, mutable.Set.empty[WarmContainer])
    printContainerMap(schedulingState.containerMap, logging)
    if (warmContainers.nonEmpty) {
      var warmIndices = IndexedSeq.empty[Int]
      var warmInvokers = IndexedSeq.empty[InvokerHealth]
      var warmDispatched = IndexedSeq.empty[ContainerSemaphore]
      for (c <- warmContainers) {
        if (!warmIndices.contains(c.invoker)){
          warmIndices = warmIndices :+ c.invoker
          warmInvokers = warmInvokers :+ invokers(c.invoker)
          warmDispatched = warmDispatched :+ dispatched(c.invoker)
        }
      }
      logging.debug(this, s"warmIndices: ${warmIndices}")
      if (warmIndices.nonEmpty) {
        lbConfig.warmPolicy match {
          case "random" =>
            val hash = fqn.asString.hashCode().abs
            val startIndex = hash % warmIndices.size
            val warmStep = 1
            val res = sophon2PolicyWarm(maxConcurrent, fqn, warmInvokers.sortWith(_.id.toInt < _.id.toInt), warmDispatched.sortWith(_.invokerId < _.invokerId), slots, startIndex, warmStep, stepsDone, currentOptimal, minCost, optimalDistance, params, lbConfig, schedulingState)
            if (res.nonEmpty) return res

          case "maxavailable" =>
            val r = scala.util.Random
            val sampleIndices = r.shuffle(warmIndices).take(lbConfig.sampleN)
            val sampleInvokers = for (i <- sampleIndices) yield invokers(i)
            val sampleDispatched = for (i <- sampleIndices) yield dispatched(i)
            val res = sophon2PolicyWarm(maxConcurrent, fqn, sampleInvokers, sampleDispatched, slots, 0, 1, 0, currentOptimal, minCost, optimalDistance, params, lbConfig, schedulingState)
            if (res.nonEmpty) return res
          case _ => throw new Exception("unknown policy config")
        }
      }
    }
    val r = scala.util.Random
    val excludedSteps = Math.max(invokers.size - lbConfig.sampleN, 0)
    sophon2PolicyCold(maxConcurrent, fqn, invokers, dispatched, slots, r.nextInt().abs % invokers.size, step, excludedSteps, currentOptimal, minCost, optimalDistance, params, lbConfig, schedulingState)

  }

    @tailrec
  def sophon2PolicyWarm(
                         maxConcurrent: Int,
                         fqn: FullyQualifiedEntityName,
                         invokers: IndexedSeq[InvokerHealth],
                         dispatched: IndexedSeq[ContainerSemaphore],
                         slots: Int,
                         index: Int,
                         step: Int,
                         stepsDone: Int, // use this as distance cost for spacial locality
                         currentOptimal: Option[(InvokerInstanceId, Boolean, String)],
                         minCost: Float,
                         optimalDistance: Int,
                         params: (Double,Double,Double,Double),
                         lbConfig: ShardingContainerPoolBalancerConfig,
                         schedulingState: ShardingContainerPoolBalancerState)(implicit logging: Logging, transId: TransactionId):
                      Option[(InvokerInstanceId, Boolean, String)] = {
        val numInvokers = invokers.size
        val invoker = invokers(index)
        val (ctnr, newCost) = if (!invoker.status.isUsable) ("", PositiveInfinity) else dispatched (index).tryAcquire2(fqn, maxConcurrent, slots, invoker.id.userMemory.toMB.toInt, params, lbConfig)
        logging.debug(this, s"${System.currentTimeMillis} step ${stepsDone}, cost ${newCost}")
        val newOptimal = if (newCost < minCost) Some(invoker.id, false, ctnr) else currentOptimal
        val newMinCost = if (newCost < minCost) newCost.toFloat else minCost
        val newDistance = if (newCost < minCost) stepsDone else optimalDistance
        if (newMinCost == 0.0) {
          return newOptimal
        }
        if (stepsDone >= numInvokers ) { // traversed all invokers
          if (newMinCost <= params._4) {
            logging.info(this, s"${System.currentTimeMillis} chosen invoker ${newOptimal.get._1.toInt}, distance ${newDistance}, totalcost ${newMinCost}")
            newOptimal
          } else {
            logging.debug(this, s"${System.currentTimeMillis} no invoker chosen, min cost ${newMinCost}")
            None }
        } else {
          val newIndex = (index + step) % numInvokers
          sophon2PolicyWarm(maxConcurrent, fqn, invokers, dispatched, slots, newIndex, step, stepsDone + 1, newOptimal, newMinCost, newDistance, params, lbConfig, schedulingState)
        }
  }

  /**
   * A modified sophon policy
   */
  @tailrec
  def sophon2PolicyCold(
                     maxConcurrent: Int,
                     fqn: FullyQualifiedEntityName,
                     invokers: IndexedSeq[InvokerHealth],
                     dispatched: IndexedSeq[ContainerSemaphore],
                     slots: Int,
                     index: Int,
                     step: Int,
                     stepsDone: Int, // use this as distance cost for spacial locality
                     currentOptimal: Option[(InvokerInstanceId, Boolean, String)],
                     minCost: Float,
                     optimalDistance: Int,
                     params: (Double,Double,Double,Double),
                     lbConfig: ShardingContainerPoolBalancerConfig,
                     schedulingState: ShardingContainerPoolBalancerState)(implicit logging: Logging, transId: TransactionId): Option[(InvokerInstanceId, Boolean, String)] = {
    // params: lbConfig.lambda, lbConfig.distanceWeight, lbConfig.evictionWeight, lbConfig.acceptThreshold
    val numInvokers = invokers.size
    logging.debug(this, s"${System.currentTimeMillis} chooseOptimal currentmin: ${minCost}")

    if (numInvokers > 0) {
      val invoker = invokers(index)
      val (ctnr, newCost) = if (!invoker.status.isUsable) ("", PositiveInfinity) else dispatched (invoker.id.toInt).tryAcquire2(fqn, maxConcurrent, slots, invoker.id.userMemory.toMB.toInt, params, lbConfig)
      logging.debug(this, s"${System.currentTimeMillis} step ${stepsDone}, cost ${newCost}")
      val newOptimal = if (newCost < minCost) Some(invoker.id, false, ctnr) else currentOptimal
      val newMinCost = if (newCost < minCost) newCost.toFloat else minCost
      val newDistance = if (newCost < minCost) stepsDone else optimalDistance
      if (stepsDone >= numInvokers ) { // traversed all invokers
        if (newMinCost <= params._4) {
          logging.info(this, s"${System.currentTimeMillis} chosen invoker ${newOptimal.get._1.toInt}, distance ${newDistance}, totalcost ${newMinCost}")
          newOptimal
        } else {
          logging.debug(this, s"${System.currentTimeMillis} no invoker chosen, min cost ${newMinCost}")
          None }
      } else {
        val newIndex = (index + step) % numInvokers
        sophon2PolicyCold(maxConcurrent, fqn, invokers, dispatched, slots, newIndex, step, stepsDone + 1, newOptimal, newMinCost, newDistance, params, lbConfig, schedulingState)
      }
    } else {
      None
    }
  }

  @tailrec
  def maxAvailablePolicy(maxConcurrent: Int,
                    fqn: FullyQualifiedEntityName,
                    invokers: IndexedSeq[InvokerHealth],
                    dispatched: IndexedSeq[ContainerSemaphore],
                    slots: Int,
                    index: Int,
                    step: Int,
                    stepsDone: Int, // use this as distance cost for spacial locality
                    currentOptimal: Option[(InvokerInstanceId, Boolean, String)],
                    maxAvailable: Int,
                    optimalDistance: Int, // not used here
                    params: (Double,Double,Double,Double))(implicit logging: Logging, transId: TransactionId): Option[(InvokerInstanceId, Boolean, String)] = {
    // params: lbConfig.lambda, lbConfig.distanceWeight, lbConfig.evictionWeight, lbConfig.acceptThreshold
    val numInvokers = invokers.size
    logging.debug(this, s"${System.currentTimeMillis} current max available ${maxAvailable}")

    if (numInvokers > 0) {
      val invoker = invokers(index)
      val available = if (!invoker.status.isUsable) 0 else dispatched (invoker.id.toInt).availablePermits
      val newOptimal = if (available > maxAvailable) {
        val (ctnr, _) = if (!invoker.status.isUsable) ("", 0) else dispatched (invoker.id.toInt).tryAcquire(fqn, maxConcurrent, slots, params)
        Some(invoker.id, false, ctnr)
      } else currentOptimal
      val newMaxAvailable = if (available > maxAvailable) {
        available
      } else maxAvailable
      if (stepsDone >= numInvokers) { // traversed all invokers
        if (newMaxAvailable >= slots) {
          logging.debug(this, s"${System.currentTimeMillis} chosen invoker ${newOptimal.get._1.toInt}, available memory ${newMaxAvailable}")
          newOptimal
        } else {
          logging.debug(this, s"${System.currentTimeMillis} no invoker chosen, max available ${newMaxAvailable}")
          None
        }
      } else {
        val newIndex = (index + step) % numInvokers
        maxAvailablePolicy(maxConcurrent, fqn, invokers, dispatched, slots, newIndex, step, stepsDone + 1, newOptimal, newMaxAvailable, optimalDistance, params)
      }
    } else {
      None
    }
  }

  @tailrec
  def maxFreePolicy(maxConcurrent: Int,
                         fqn: FullyQualifiedEntityName,
                         invokers: IndexedSeq[InvokerHealth],
                         dispatched: IndexedSeq[ContainerSemaphore],
                         slots: Int,
                         index: Int,
                         step: Int,
                         stepsDone: Int, // use this as distance cost for spacial locality
                         currentOptimal: Option[(InvokerInstanceId, Boolean, String)],
                         maxFree: Int,
                         optimalDistance: Int, // not used here
                         params: (Double,Double,Double,Double))(implicit logging: Logging, transId: TransactionId): Option[(InvokerInstanceId, Boolean, String)] = {
    // params: lbConfig.lambda, lbConfig.distanceWeight, lbConfig.evictionWeight, lbConfig.acceptThreshold
    val numInvokers = invokers.size
    logging.debug(this, s"${System.currentTimeMillis} current max free ${maxFree}")

    if (numInvokers > 0) {
      val invoker = invokers(index)
      val free = if (!invoker.status.isUsable) 0 else dispatched (invoker.id.toInt).freePermits
      val newOptimal = if (free > maxFree) {
        val (ctnr, _) = if (!invoker.status.isUsable) ("", 0) else dispatched (invoker.id.toInt).tryAcquire(fqn, maxConcurrent, slots, params)
        Some(invoker.id, false, ctnr)
      } else currentOptimal
      val newMaxFree = if (free > maxFree) {
        free
      } else maxFree
      if (stepsDone >= numInvokers) { // traversed all invokers
        if (newMaxFree >= slots) {
          logging.debug(this, s"${System.currentTimeMillis} chosen invoker ${newOptimal.get._1.toInt}, free memory ${newMaxFree}")
          newOptimal
        } else {
          logging.debug(this, s"${System.currentTimeMillis} no invoker chosen, max free ${newMaxFree}")
          None
        }
      } else {
        val newIndex = (index + step) % numInvokers
        maxFreePolicy(maxConcurrent, fqn, invokers, dispatched, slots, newIndex, step, stepsDone + 1, newOptimal, newMaxFree, optimalDistance, params)
      }
    } else {
      None
    }
  }

  @tailrec
  def vanillaPolicy(maxConcurrent: Int,
                         fqn: FullyQualifiedEntityName,
                         invokers: IndexedSeq[InvokerHealth],
                         dispatched: IndexedSeq[ContainerSemaphore],
                         slots: Int,
                         index: Int,
                         step: Int,
                         stepsDone: Int, // use this as distance cost for spacial locality
                         params: (Double,Double,Double,Double))(implicit logging: Logging, transId: TransactionId): Option[(InvokerInstanceId, Boolean, String)] = {
    // params: lbConfig.lambda, lbConfig.distanceWeight, lbConfig.evictionWeight, lbConfig.acceptThreshold
    val numInvokers = invokers.size
    logging.debug(this, s"${System.currentTimeMillis} current steps ${stepsDone}")

    if (numInvokers > 0) {
      val invoker = invokers(index)
      val available = if (!invoker.status.isUsable) 0 else dispatched (invoker.id.toInt).availablePermits
      if (available >= slots) {
        val (ctnr, _) = if (!invoker.status.isUsable) ("", 0) else dispatched(invoker.id.toInt).tryAcquire(fqn, maxConcurrent, slots, params)
        Some(invoker.id, false, ctnr)
      } else {
        if (stepsDone >= numInvokers) { // traversed all invokers
          None
        } else {
          val newIndex = (index + step) % numInvokers
          vanillaPolicy(maxConcurrent, fqn, invokers, dispatched, slots, newIndex, step, stepsDone + 1, params)
        }
      }
    } else {
      None
    }
  }

  @tailrec
  def stickyAvailablePolicy(maxConcurrent: Int,
                            fqn: FullyQualifiedEntityName,
                            invokers: IndexedSeq[InvokerHealth],
                            dispatched: IndexedSeq[ContainerSemaphore],
                            slots: Int,
                            index: Int,
                            step: Int,
                            stepsDone: Int, // use this as distance cost for spacial locality
                            currentOptimal: Option[(InvokerInstanceId, Boolean, String)],
                            minCost: Float,
                            optimalDistance: Int,
                            params: (Double, Double, Double, Double))(implicit logging: Logging, transId: TransactionId): Option[(InvokerInstanceId, Boolean, String)] = {
    // params: lbConfig.lambda, lbConfig.distanceWeight, lbConfig.evictionWeight, lbConfig.acceptThreshold
    val numInvokers = invokers.size
    logging.debug(this, s"${System.currentTimeMillis} current steps ${stepsDone}, minCost ${minCost}")

    if (numInvokers > 0) {
      val invoker = invokers(index)
      val available = if (!invoker.status.isUsable) 0 else dispatched(invoker.id.toInt).availablePermits
      val load = 1 - (available.toFloat / invoker.id.userMemory.toMB.toInt)
      val cost = if (available >= slots) (stepsDone / numInvokers) * params._2 + load * (1 - params._2) else PositiveInfinity
      val newOptimal = if (cost < minCost) {
        val (ctnr, _) = if (!invoker.status.isUsable) ("", 0) else dispatched(invoker.id.toInt).tryAcquire(fqn, maxConcurrent, slots, params)
        Some(invoker.id, false, ctnr)
      } else {
        currentOptimal
      }
      val newDistance = if (cost < minCost) stepsDone else optimalDistance
      val newCost = if (cost < minCost) cost.toFloat else minCost
      if (stepsDone >= numInvokers || (stepsDone+1)*params._2 >= newCost)  { // traversed all invokers or current optimal cost must be lower than next invoker
        if (newCost < PositiveInfinity) {
          logging.debug(this, s"${System.currentTimeMillis} chosen invoker ${newOptimal.get._1.toInt}, distance ${newDistance}, cost ${newCost}")
          newOptimal
        } else {
          logging.info(this, s"${System.currentTimeMillis} no invoker chosen")
          None
        }
      } else {
        val newIndex = (index + step) % numInvokers
        stickyAvailablePolicy(maxConcurrent, fqn, invokers, dispatched, slots, newIndex, step, stepsDone + 1, newOptimal, newCost, newDistance, params)
      }
    } else {
      None
    }
  }

  @tailrec
  def stickyFreePolicy(maxConcurrent: Int,
                            fqn: FullyQualifiedEntityName,
                            invokers: IndexedSeq[InvokerHealth],
                            dispatched: IndexedSeq[ContainerSemaphore],
                            slots: Int,
                            index: Int,
                            step: Int,
                            stepsDone: Int, // use this as distance cost for spacial locality
                            currentOptimal: Option[(InvokerInstanceId, Boolean, String)],
                            minCost: Float,
                            optimalDistance: Int,
                            params: (Double, Double, Double, Double))(implicit logging: Logging, transId: TransactionId): Option[(InvokerInstanceId, Boolean, String)] = {
    // params: lbConfig.lambda, lbConfig.distanceWeight, lbConfig.evictionWeight, lbConfig.acceptThreshold
    val numInvokers = invokers.size
    logging.debug(this, s"${System.currentTimeMillis} current steps ${stepsDone}")

    if (numInvokers > 0) {
      val invoker = invokers(index)
      val available = if (!invoker.status.isUsable) 0 else dispatched(invoker.id.toInt).availablePermits
      val free = if (!invoker.status.isUsable) 0 else dispatched(invoker.id.toInt).freePermits
      val load = 1 - (free.toFloat / invoker.id.userMemory.toMB.toInt)
      val cost = if (available >= slots) (stepsDone / numInvokers) * params._2 + load * (1 - params._2) else PositiveInfinity
      val newOptimal = if (cost < minCost) {
        val (ctnr, _) = if (!invoker.status.isUsable) ("", 0) else dispatched(invoker.id.toInt).tryAcquire(fqn, maxConcurrent, slots, params)
        Some(invoker.id, false, ctnr)
      } else {
        currentOptimal
      }
      val newDistance = if (cost < minCost) stepsDone else optimalDistance
      val newCost = if (cost < minCost) cost.toFloat else minCost
      if (stepsDone >= numInvokers || (stepsDone+1)*params._2 >= newCost)  { // traversed all invokers or current optimal cost must be lower than next invoker
        if (newCost < PositiveInfinity) {
          logging.debug(this, s"${System.currentTimeMillis} chosen invoker ${newOptimal.get._1.toInt}, distance ${newDistance}")
          newOptimal
        } else {
          logging.info(this, s"${System.currentTimeMillis} no invoker chosen")
          None
        }
      } else {
        val newIndex = (index + step) % numInvokers
        stickyFreePolicy(maxConcurrent, fqn, invokers, dispatched, slots, newIndex, step, stepsDone + 1, newOptimal, newCost, newDistance, params)
      }
    } else {
      None
    }
  }

  @tailrec
  def randomPolicy(maxConcurrent: Int,
                    fqn: FullyQualifiedEntityName,
                    invokers: IndexedSeq[InvokerHealth],
                    dispatched: IndexedSeq[ContainerSemaphore],
                    slots: Int,
                    index: Int,
                    step: Int,
                    stepsDone: Int, // use this as distance cost for spacial locality
                    params: (Double,Double,Double,Double))(implicit logging: Logging, transId: TransactionId): Option[(InvokerInstanceId, Boolean, String)] = {
    // params: lbConfig.lambda, lbConfig.distanceWeight, lbConfig.evictionWeight, lbConfig.acceptThreshold
    val numInvokers = invokers.size
    logging.debug(this, s"${System.currentTimeMillis} current steps ${stepsDone}")

    if (numInvokers > 0) {
      val invoker = invokers(index)
      val available = if (!invoker.status.isUsable) 0 else dispatched (invoker.id.toInt).availablePermits
      if (available >= slots) {
        val (ctnr, _) = if (!invoker.status.isUsable) ("", 0) else dispatched(invoker.id.toInt).tryAcquire(fqn, maxConcurrent, slots, params)
        Some(invoker.id, false, ctnr)
      } else {
        if (stepsDone >= numInvokers) { // traversed all invokers
          None
        } else {
          val newIndex = (index + step) % numInvokers
          randomPolicy(maxConcurrent, fqn, invokers, dispatched, slots, newIndex, step, stepsDone + 1, params)
        }
      }
    } else {
      None
    }
  }

}

/**
 * Holds the state necessary for scheduling of actions.
 *
 * @param _invokers all of the known invokers in the system
 * @param _managedInvokers all invokers for managed runtimes
 * @param _blackboxInvokers all invokers for blackbox runtimes
 * @param _managedStepSizes the step-sizes possible for the current managed invoker count
 * @param _blackboxStepSizes the step-sizes possible for the current blackbox invoker count
 * @param _invokerSlots state of accessible slots of each invoker
 */
case class ShardingContainerPoolBalancerState(
  private var _invokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
  private var _managedInvokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
  private var _blackboxInvokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
  private var _managedStepSizes: Seq[Int] = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(0),
  private var _blackboxStepSizes: Seq[Int] = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(0),
  protected[loadBalancer] var _invokerSlots: IndexedSeq[ContainerSemaphore] =
    IndexedSeq.empty[ContainerSemaphore],
  protected[loadBalancer] var containerMap: ConcurrentHashMap[FullyQualifiedEntityName, mutable.Set[WarmContainer]] =
  new ConcurrentHashMap[FullyQualifiedEntityName, mutable.Set[WarmContainer]],// action -> containers
  private var _clusterSize: Int = 1)(
  lbConfig: ShardingContainerPoolBalancerConfig =
    loadConfigOrThrow[ShardingContainerPoolBalancerConfig](ConfigKeys.loadbalancer))(implicit logging: Logging) {

  // Managed fraction and blackbox fraction can be between 0.0 and 1.0. The sum of these two fractions has to be between
  // 1.0 and 2.0.
  // If the sum is 1.0 that means, that there is no overlap of blackbox and managed invokers. If the sum is 2.0, that
  // means, that there is no differentiation between managed and blackbox invokers.
  // If the sum is below 1.0 with the initial values from config, the blackbox fraction will be set higher than
  // specified in config and adapted to the managed fraction.
  private val managedFraction: Double = Math.max(0.0, Math.min(1.0, lbConfig.managedFraction))
  private val blackboxFraction: Double = Math.max(1.0 - managedFraction, Math.min(1.0, lbConfig.blackboxFraction))
  logging.info(this, s"managedFraction = $managedFraction, blackboxFraction = $blackboxFraction")(
    TransactionId.loadbalancer)

  /** Getters for the variables, setting from the outside is only allowed through the update methods below */
  def invokers: IndexedSeq[InvokerHealth] = _invokers
  def managedInvokers: IndexedSeq[InvokerHealth] = _managedInvokers
  def blackboxInvokers: IndexedSeq[InvokerHealth] = _blackboxInvokers
  def managedStepSizes: Seq[Int] = _managedStepSizes
  def blackboxStepSizes: Seq[Int] = _blackboxStepSizes
  def invokerSlots: IndexedSeq[ContainerSemaphore] = _invokerSlots
  def clusterSize: Int = _clusterSize

  /**
   * @param memory
   * @return calculated invoker slot
   */
  private def getInvokerSlot(memory: ByteSize): ByteSize = {
    val invokerShardMemorySize = memory / _clusterSize
    val newTreshold = if (invokerShardMemorySize < MemoryLimit.MIN_MEMORY) {
      logging.error(
        this,
        s"registered controllers: calculated controller's invoker shard memory size falls below the min memory of one action. "
          + s"Setting to min memory. Expect invoker overloads. Cluster size ${_clusterSize}, invoker user memory size ${memory.toMB.MB}, "
          + s"min action memory size ${MemoryLimit.MIN_MEMORY.toMB.MB}, calculated shard size ${invokerShardMemorySize.toMB.MB}.")(
        TransactionId.loadbalancer)
      MemoryLimit.MIN_MEMORY
    } else {
      invokerShardMemorySize
    }
    newTreshold
  }

  /**
   * Updates the scheduling state with the new invokers.
   *
   * This is okay to not happen atomically since dirty reads of the values set are not dangerous. It is important though
   * to update the "invokers" variables last, since they will determine the range of invokers to choose from.
   *
   * Handling a shrinking invokers list is not necessary, because InvokerPool won't shrink its own list but rather
   * report the invoker as "Offline".
   *
   * It is important that this method does not run concurrently to itself and/or to [[updateCluster]]
   */
  def updateInvokers(newInvokers: IndexedSeq[InvokerHealth]): Unit = {
    val oldSize = _invokers.size
    val newSize = newInvokers.size

    // for small N, allow the managed invokers to overlap with blackbox invokers, and
    // further assume that blackbox invokers << managed invokers
    val managed = Math.max(1, Math.ceil(newSize.toDouble * managedFraction).toInt)
    val blackboxes = Math.max(1, Math.floor(newSize.toDouble * blackboxFraction).toInt)

    _invokers = newInvokers
    _managedInvokers = _invokers.take(managed)
    _blackboxInvokers = _invokers.takeRight(blackboxes)

    val logDetail = if (oldSize != newSize) {
      _managedStepSizes = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(managed)
      _blackboxStepSizes = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(blackboxes)

      if (oldSize < newSize) {
        // Keeps the existing state..
        val onlyNewInvokers = _invokers.drop(_invokerSlots.length)
        _invokerSlots = _invokerSlots ++ onlyNewInvokers.map { invoker =>
          new ContainerSemaphore(getInvokerSlot(invoker.id.userMemory).toMB.toInt, logging, invoker.id.toInt, this)
        }
        val newInvokerDetails = onlyNewInvokers
          .map(i =>
            s"${i.id.toString}: ${i.status} / ${getInvokerSlot(i.id.userMemory).toMB.MB} of ${i.id.userMemory.toMB.MB}")
          .mkString(", ")
        s"number of known invokers increased: new = $newSize, old = $oldSize. details: $newInvokerDetails."
      } else {
        s"number of known invokers decreased: new = $newSize, old = $oldSize."
      }
    } else {
      s"no update required - number of known invokers unchanged: $newSize."
    }

    logging.info(
      this,
      s"loadbalancer invoker status updated. managedInvokers = $managed blackboxInvokers = $blackboxes. $logDetail")(
      TransactionId.loadbalancer)
  }

  /**
   * Updates the size of a cluster. Throws away all state for simplicity.
   *
   * This is okay to not happen atomically, since a dirty read of the values set are not dangerous. At worst the
   * scheduler works on outdated invoker-load data which is acceptable.
   *
   * It is important that this method does not run concurrently to itself and/or to [[updateInvokers]]
   */
  def updateCluster(newSize: Int): Unit = {
    val actualSize = newSize max 1 // if a cluster size < 1 is reported, falls back to a size of 1 (alone)
    if (_clusterSize != actualSize) {
      val oldSize = _clusterSize
      _clusterSize = actualSize
      _invokerSlots = _invokers.map { invoker =>
        new ContainerSemaphore(getInvokerSlot(invoker.id.userMemory).toMB.toInt, logging, invoker.id.toInt, this)
      }
      // Directly after startup, no invokers have registered yet. This needs to be handled gracefully.
      val invokerCount = _invokers.size
      val totalInvokerMemory =
        _invokers.foldLeft(0L)((total, invoker) => total + getInvokerSlot(invoker.id.userMemory).toMB).MB
      val averageInvokerMemory =
        if (totalInvokerMemory.toMB > 0 && invokerCount > 0) {
          (totalInvokerMemory / invokerCount).toMB.MB
        } else {
          0.MB
        }
      logging.info(
        this,
        s"loadbalancer cluster size changed from $oldSize to $actualSize active nodes. ${invokerCount} invokers with ${averageInvokerMemory} average memory size - total invoker memory ${totalInvokerMemory}.")(
        TransactionId.loadbalancer)
    }
  }
}

/**
 * Configuration for the cluster created between loadbalancers.
 *
 * @param useClusterBootstrap Whether or not to use a bootstrap mechanism
 */
case class ClusterConfig(useClusterBootstrap: Boolean)

/**
 * Configuration for the sharding container pool balancer.
 *
 * @param blackboxFraction the fraction of all invokers to use exclusively for blackboxes
 * @param timeoutFactor factor to influence the timeout period for forced active acks (time-limit.std * timeoutFactor + timeoutAddon)
 * @param timeoutAddon extra time to influence the timeout period for forced active acks (time-limit.std * timeoutFactor + timeoutAddon)
 */
case class ShardingContainerPoolBalancerConfig(overloadBehavior: String, // "force" or "queue", sophon only supports "queue"
                                               managedFraction: Double,
                                               blackboxFraction: Double,
                                               timeoutFactor: Int,
                                               timeoutAddon: FiniteDuration,
                                               lambda: Double,
                                               distanceWeight: Double,
                                               evictionWeight: Double,
                                               acceptThreshold: Double,
                                               bpdistanceWeight: Double,
                                               bpevictionWeight: Double,
                                               bpacceptThreshold: Double,
                                               bpLambda: Double,
                                               schedulingPolicy: String,
                                               warmPolicy: String="",
                                               coldPolicy: String="",
                                               sampleN: Int)

/**
 * State kept for each activation slot until completion.
 *
 * @param id id of the activation
 * @param namespaceId namespace that invoked the action
 * @param invokerName invoker the action is scheduled to
 * @param memoryLimit memory limit of the invoked action
 * @param timeLimit time limit of the invoked action
 * @param maxConcurrent concurrency limit of the invoked action
 * @param fullyQualifiedEntityName fully qualified name of the invoked action
 * @param timeoutHandler times out completion of this activation, should be canceled on good paths
 * @param isBlackbox true if the invoked action is a blackbox action, otherwise false (managed action)
 * @param isBlocking true if the action is invoked in a blocking fashion, i.e. "somebody" waits for the result
 */
case class ActivationEntry(id: ActivationId,
                           namespaceId: UUID,
                           invokerName: InvokerInstanceId,
                           memoryLimit: ByteSize,
                           timeLimit: FiniteDuration,
                           maxConcurrent: Int,
                           fullyQualifiedEntityName: FullyQualifiedEntityName,
                           timeoutHandler: Cancellable,
                           isBlackbox: Boolean,
                           isBlocking: Boolean,
                           container: String = "")

class WarmContainer(ctnName: String, used: Instant, memoryLimit: Int, actionid: FullyQualifiedEntityName, decayLambda: Float, invokerId: Int) {
  def name = ctnName
  def lastUsed = used
  def memory = memoryLimit
  def action = actionid
  def lambda = decayLambda
  def invoker = invokerId
  override def toString: String = s"${ctnName} ${lastUsed}, ${memory}MB, ${action}, ${invoker}"
}

/**
 * Updated semaphore considering warm container. Support concurrency = 1 only.
 */
class ContainerSemaphore(memoryPermits: Int, logging: Logging, val invokerId: Int, state: ShardingContainerPoolBalancerState) extends ForcibleSemaphore(memoryPermits) {

  protected[loadBalancer] val pausedContainers = mutable.Map.empty[String, WarmContainer]
  protected[loadBalancer] val inFlightContainers = mutable.Map.empty[String, WarmContainer]

  val containerTimeout: Long = 10 * 60 * 1000
  val protectTimeInMs: Long = 4000

  def freePermits: Int = {
    pausedContainers.synchronized(availablePermits - pausedContainers.foldLeft(0)((p, cont) => p + cont._2.memory))
  }

  /**
   * Acquire tried resource, since we've been holding the lock, should success (except container is no longer available, updated by release message)
   * @param actionid
   * @param slots
   * @param container
   * @return container chosen
   */
  final def acquire(actionid: FullyQualifiedEntityName, slots: Int, container: String): String = {
      container match {
        case "" => // cold start
          val freeMemory = super.availablePermits - pausedContainers.synchronized(pausedContainers.foldLeft(0L)((total, cur) => total + cur._2.memory))
          logging.debug(this, s"creating cold container of ${slots} MB from ${freeMemory} MB free memory")
          evict(slots - freeMemory)
          super.forceAcquire(slots)
          ""
        case _ => // warm start
          logging.debug(this, s"using warm container ${container}")
          pausedContainers.synchronized(pausedContainers.get(container)) match {
            case Some(found) =>
              pausedContainers.synchronized(pausedContainers.remove(container))
              logging.info(this, s"warm start on invoker${invokerId}/${container}")
              removeFromMap(found)
              inFlightContainers.synchronized(inFlightContainers(container) = found)
              super.forceAcquire(slots)
              container
            case None =>
              logging.warn(this, s"acquire non-exist container ${container}")
              super.forceAcquire(slots)
              ""
          }
      }
  }

  /**
   * @param actionid
   * @param maxConcurrent
   * @param memoryPermits
   * @return container, and cost
   */
  final def tryAcquire(actionid: FullyQualifiedEntityName, maxConcurrent: Int, memoryPermits: Int, params:(Double,Double,Double,Double)): (String, Float) = {
    // params: lambda, distanceWeight, evictionWeight, acceptThreshold
    val binaryCost = { (container: WarmContainer) =>
      if (System.currentTimeMillis() < container.lastUsed.toEpochMilli + protectTimeInMs) PositiveInfinity else 0F
    }

    val expCost = { container: WarmContainer =>
      exp(- ((System.currentTimeMillis() - container.lastUsed.toEpochMilli)/1000.0)*container.lambda).toFloat
    }

    actionid match {
//      case FullyQualifiedEntityName(_, name, _, _) if name.toString.startsWith("burst_parallel") => tryAcquireNormal(actionid, maxConcurrent, memoryPermits, expCost)
      case _ => tryAcquireNormal(actionid, maxConcurrent, memoryPermits, expCost)
    }
  }


  final def tryAcquireNormal(actionid: FullyQualifiedEntityName, maxConcurrent: Int, memoryPermits: Int, costFunction: WarmContainer => Float): (String, Float) = {
    // assume maxConcurrent == 1 for now
    val debug = false
    pausedContainers.synchronized {
      if (super.availablePermits < memoryPermits) {
        if (debug) logging.debug(this,s"${System.currentTimeMillis} not enough memory for action ${actionid}")
        return ("", PositiveInfinity)
      }
      // clean up timeouts
      pausedContainers.retain((_, c)=>c.lastUsed.isAfter(Instant.now.minusMillis(containerTimeout)))
      if (debug) logging.debug(this,s"${System.currentTimeMillis} try to acquire ${memoryPermits} MB, current containers: ${pausedContainers}")
      // current action's containers
      val warmContainers = pausedContainers.filter(_._2.action==actionid)
      if (warmContainers.nonEmpty) { // should have a warm start
        if (debug) logging.debug(this,s"${System.currentTimeMillis} a warm container for ${actionid}")
        val chosen = warmContainers.minBy(_._2.lastUsed) // changed invoker warm policy
        return (chosen._1, 0F)
      }
      // cold start, check memory
      val freeMemory = super.availablePermits - pausedContainers.foldLeft(0L)((total, cur) => total + cur._2.memory)
      if (freeMemory < memoryPermits) { // need eviction
        if (debug) logging.debug(this,s"${System.currentTimeMillis} not enough free memory for action ${actionid}, may evict")
        return ("", tryEvict(pausedContainers.clone(), memoryPermits - freeMemory, costFunction, 0F))
      }
      // all other cold starts should work
      if (debug) logging.debug(this,s"${System.currentTimeMillis} a cold start for action ${actionid}")
      return ("", 0F)
    }
  }

  final def tryAcquire2(actionid: FullyQualifiedEntityName, maxConcurrent: Int, memoryPermits: Int, totalPermits: Int, params:(Double,Double,Double,Double), lbConfig: ShardingContainerPoolBalancerConfig): (String, Float) = {
    // params: lambda, distanceWeight, evictionWeight, acceptThreshold
    val binaryCost = { (container: WarmContainer) =>
      if (System.currentTimeMillis() < container.lastUsed.toEpochMilli + protectTimeInMs) PositiveInfinity else 0F
    }

    // warm 0 ~ 1/4
    // cold 1/4+
    def warmCost (load: Float) = lbConfig.warmPolicy match {
      case "maxavailable" => load/4
      case "random" => 0F
      case "truerandom" => scala.util.Random.nextFloat()/4
      case _ => throw new Exception("unknown policy config")
    }

    def coldNoEvictCost (load: Float) = lbConfig.coldPolicy match {
      case "maxavailable" => load/4 + 1F/4
      case "mincost" => 1F/4
      case _ => throw new Exception("unknown policy config")
    }

    def coldEvictCost (load: Float, cost: Float) = lbConfig.coldPolicy match {
      case "maxavailable" => load/4 + 1F/4
      case "mincost" => 1F/4 + cost/2
      case _ => throw new Exception("unknown policy config")
    }

    val expCost = { container: WarmContainer =>
      exp(- ((System.currentTimeMillis() - container.lastUsed.toEpochMilli)/1000.0)*container.lambda).toFloat
    }

    actionid match {
      case _ => tryAcquireNormal2(actionid, maxConcurrent, memoryPermits, totalPermits, expCost, warmCost, coldNoEvictCost, coldEvictCost)
    }
  }

  final def tryAcquireNormal2(actionid: FullyQualifiedEntityName, maxConcurrent: Int, memoryPermits: Int, totalPermits: Int, costFunction: WarmContainer => Float,
                             warmCost: Float => Float, coldNoEvictCost: Float => Float, coldEvictCost: (Float, Float) => Float): (String, Float) = {
    // assume maxConcurrent == 1 for now
    val debug = false
      val load = 1 - (super.availablePermits.toFloat / totalPermits)
      if (super.availablePermits < memoryPermits) {
        if (debug) logging.debug(this,s"${System.currentTimeMillis} not enough memory for action ${actionid}")
        return ("", PositiveInfinity)
      }
      // clean up timeouts
      pausedContainers.synchronized(pausedContainers.retain((_, c)=>c.lastUsed.isAfter(Instant.now.minusMillis(containerTimeout)))) // right now no worry about the map
      if (debug) logging.debug(this,s"${System.currentTimeMillis} try to acquire ${memoryPermits} MB, current containers: ${pausedContainers}")
      // current action's containers
      val warmContainers = pausedContainers.synchronized(pausedContainers.filter(_._2.action==actionid))
      if (warmContainers.nonEmpty) { // should have a warm start
        if (debug) logging.debug(this,s"${System.currentTimeMillis} a warm container for ${actionid}")
        val chosen = warmContainers.minBy(_._2.lastUsed) // changed invoker warm policy
        return (chosen._1, warmCost(load))
      }
      // cold start, check memory
      val freeMemory = super.availablePermits - pausedContainers.synchronized(pausedContainers.foldLeft(0L)((total, cur) => total + cur._2.memory))
      if (freeMemory < memoryPermits) { // need eviction
        if (debug) logging.debug(this,s"${System.currentTimeMillis} not enough free memory for action ${actionid}, may evict")
        return ("", coldEvictCost(load, tryEvict(pausedContainers.synchronized(pausedContainers.clone()), memoryPermits - freeMemory, costFunction, 0F)))
      }
      // all other cold starts should work
      if (debug) logging.debug(this,s"${System.currentTimeMillis} a cold start for action ${actionid}")
      return ("", coldNoEvictCost(load))

  }

  @tailrec
  final def evict(needEvict: Long): Unit = {
    var lru : (String, WarmContainer) = null
    if (needEvict <= 0) return
    try {
      lru = pausedContainers.synchronized(pausedContainers.minBy(_._2.lastUsed))
    } catch {
      case e: Exception => return
    }
    pausedContainers.synchronized(pausedContainers.remove(lru._1))
    logging.info(this, s"evicting invoker${invokerId}/${lru._1}")
    removeFromMap(lru._2)
    logging.debug(this, s"evicting ${lru._1} for ${lru._2.memory} MB")
    evict(needEvict - lru._2.memory)
  }

  @tailrec
  final def tryEvict(remainingContainers: mutable.Map[String, WarmContainer], needEvict: Long, costFunc: WarmContainer => Float, costSoFar: Float): Float = {
    var lru : (String, WarmContainer) = null
    if (needEvict <= 0) return costSoFar
    try {
      lru = remainingContainers.minBy(_._2.lastUsed)
    } catch {
      case e: Exception => return PositiveInfinity
    }
    remainingContainers.remove(lru._1)
    tryEvict(remainingContainers, needEvict - lru._2.memory, costFunc, costSoFar + costFunc(lru._2))
  }


  def releaseConcurrent(actionid: FullyQualifiedEntityName, maxConcurrent: Int, memoryPermits: Int, schedContainer: String, usedContainer: String, lastUsed: Instant, lambda: Float): Unit = {
    val debug = false
    var newCont : WarmContainer = null
    if (debug) logging.info(this, s"releasingConcurrent, schedContainer: ${schedContainer}, usedContainer: ${usedContainer}, lastUsed: ${lastUsed.toEpochMilli}")
    require(memoryPermits > 0, "cannot release negative or no permits")
    logging.info(this, s"release ${memoryPermits} MB")
      super.release(memoryPermits)
      (schedContainer, usedContainer) match {
        case ("", used) if used != "" =>
        // cold start
        case (sched, used) if used == sched =>
          // correct warm start
          if (inFlightContainers.synchronized(inFlightContainers.remove(sched).isEmpty))  logging.info(this, s"warm container not found 1")
        case (sched, used) if used != sched =>
          // incorrect warm start
          val schedCon = inFlightContainers.synchronized(inFlightContainers.remove(sched)) // interestingly, because sched container not found, it may get evicted already
          //          if (schedCon.isEmpty) logging.info(this, s"warm container not found 2")
          //          else containerPool(sched) = schedCon.get
          logging.warn(this, s"incorrect warm container guess: ${sched} -> ${used}")
      }
      newCont = new WarmContainer(usedContainer, lastUsed, memoryPermits, actionid, lambda, invokerId)
      pausedContainers.synchronized(pausedContainers(usedContainer) = newCont)

    val set = state.containerMap.getOrDefault(actionid, mutable.Set.empty[WarmContainer])
    state.containerMap.synchronized{
      set.add(newCont)
      state.containerMap.put(actionid, set)
    }
    logging.info(this, s"after adding invoker${invokerId}/${usedContainer}")
    ShardingContainerPoolBalancer.printContainerMap(state.containerMap, logging)
  }

  def removeContainer(container: String): Unit = {
      logging.info(this, s"removeContainer(container=${container})")
      val someCont = pausedContainers.synchronized(pausedContainers.remove(container))
    if (someCont.nonEmpty) {
        logging.warn(this, s"invoker removed container invoker${invokerId}/${container} but still found in warm pool")
        removeFromMap(someCont.get)
      }
      val someInFlight = inFlightContainers.synchronized(inFlightContainers.remove(container))
    if (someInFlight.nonEmpty) {
        logging.warn(this, s"invoker removed container invoker${invokerId}/${container} but has pending invocations to it")
        removeFromMap(someInFlight.get)
      }
  }

  def removeFromMap(container: WarmContainer): Unit = {
    val set = state.containerMap.getOrDefault(container.action, mutable.Set.empty[WarmContainer])
    set.synchronized{set.remove(container)}
    ShardingContainerPoolBalancer.printContainerMap(state.containerMap, logging)
  }
}
