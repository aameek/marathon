package mesosphere.marathon.core.matcher.impl

import akka.actor.{ ActorLogging, Actor, Props, ActorRef }
import akka.pattern.pipe
import akka.event.LoggingReceive
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.matcher.OfferMatcher
import mesosphere.marathon.core.matcher.impl.OfferMatcherMultiplexerActor.OfferData
import mesosphere.marathon.core.matcher.util.ActorOfferMatcher
import mesosphere.marathon.core.task.bus.TaskStatusObservable.TaskStatusUpdate
import mesosphere.marathon.core.task.bus.{ MarathonTaskStatus, TaskStatusEmitter }
import mesosphere.marathon.state.Timestamp
import mesosphere.marathon.tasks.ResourceUtil
import org.apache.mesos.Protos.{ Offer, OfferID, Resource, TaskInfo }
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.immutable.Queue
import scala.util.Random
import scala.util.control.NonFatal

/**
  * The OfferMatcherMultiplexer actor offers one interface to a
  * dynamic collection of matchers.
  */
private[impl] object OfferMatcherMultiplexerActor {
  def props(random: Random, clock: Clock, taskStatusEmitter: TaskStatusEmitter): Props = {
    Props(new OfferMatcherMultiplexerActor(random, clock, taskStatusEmitter))
  }

  case class SetTaskLaunchTokens(tokens: Int)

  private val log = LoggerFactory.getLogger(getClass)
  private case class OfferData(
      offer: Offer,
      deadline: Timestamp,
      sender: ActorRef,
      consumerQueue: Queue[OfferMatcher],
      tasks: Seq[TaskInfo]) {
    def addConsumer(consumer: OfferMatcher): OfferData = copy(consumerQueue = consumerQueue.enqueue(consumer))
    def nextConsumerOpt: Option[(OfferMatcher, OfferData)] = {
      consumerQueue.dequeueOption map {
        case (nextConsumer, newQueue) => nextConsumer -> copy(consumerQueue = newQueue)
      }
    }

    def addTasks(added: Seq[TaskInfo]): OfferData = {
      val offerResources: Seq[Resource] = offer.getResourcesList.asScala
      val taskResources: Seq[Resource] = added.flatMap(_.getResourcesList.asScala)
      val leftOverResources = ResourceUtil.consumeResources(offerResources, taskResources)
      val leftOverOffer = offer.toBuilder.clearResources().addAllResources(leftOverResources.asJava).build()
      copy(
        offer = leftOverOffer,
        tasks = added ++ tasks
      )
    }
  }
}

private class OfferMatcherMultiplexerActor private (random: Random, clock: Clock, taskStatusEmitter: TaskStatusEmitter)
    extends Actor with ActorLogging {
  private[this] var launchTokens: Int = 0

  private[this] var livingMatchers: Set[OfferMatcher] = Set.empty
  private[this] var offerQueues: Map[OfferID, OfferMatcherMultiplexerActor.OfferData] = Map.empty

  override def receive: Receive = LoggingReceive {
    Seq[Receive](
      setLaunchTokens,
      changingConsumers,
      receiveProcessOffer,
      processLaunchTasks
    ).reduceLeft(_.orElse[Any, Unit](_))
  }

  private[this] def setLaunchTokens: Receive = {
    case OfferMatcherMultiplexerActor.SetTaskLaunchTokens(tokens) => launchTokens = tokens
  }

  private[this] def changingConsumers: Receive = {
    case ActorOfferMatcherManager.AddMatcher(consumer) =>
      livingMatchers += consumer
      offerQueues.mapValues(_.addConsumer(consumer))
      sender() ! ActorOfferMatcherManager.MatcherAdded(consumer)

    case ActorOfferMatcherManager.RemoveMatcher(consumer) =>
      livingMatchers -= consumer
      sender() ! ActorOfferMatcherManager.MatcherRemoved(consumer)
  }

  private[this] def receiveProcessOffer: Receive = {
    case ActorOfferMatcher.MatchOffer(deadline, offer: Offer) if livingMatchers.isEmpty =>
      log.debug(s"Ignoring offer ${offer.getId.getValue}: No one interested.")
      sender() ! OfferMatcher.MatchedTasks(offer.getId, Seq.empty)

    case processOffer @ ActorOfferMatcher.MatchOffer(deadline, offer: Offer) =>
      log.info(s"Start processing offer ${offer.getId.getValue}")

      // setup initial offer data
      val randomizedConsumers = random.shuffle(livingMatchers).to[Queue]
      val data = OfferMatcherMultiplexerActor.OfferData(offer, deadline, sender(), randomizedConsumers, Seq.empty)
      offerQueues += offer.getId -> data

      // deal with the timeout
      import context.dispatcher
      context.system.scheduler.scheduleOnce(
        clock.now().until(deadline),
        self,
        OfferMatcher.MatchedTasks(offer.getId, Seq.empty))

      // process offer for the first time
      scheduleNextMatcherOrFinish(data)
  }

  private[this] def processLaunchTasks: Receive = {
    case OfferMatcher.MatchedTasks(offerId, tasks) => continueWithOffer(offerId, tasks)
  }

  private[this] def continueWithOffer(offerId: OfferID, addedTasks: Seq[TaskInfo]): Unit = {
    def processAddedTasks(data: OfferData): OfferData = {
      val dataWithTasks = try {
        val (launchTasks, declineTasks) = addedTasks.splitAt(Seq(launchTokens, addedTasks.size).min)

        declineTasks.foreach { declineTask =>
          val ts = Timestamp.now()
          val status = new TaskStatusUpdate(ts, declineTask.getTaskId, MarathonTaskStatus.LaunchDenied)
          taskStatusEmitter.publish(status)
        }

        val newData: OfferData = data.addTasks(launchTasks)
        launchTokens -= launchTasks.size
        newData
      }
      catch {
        case NonFatal(e) =>
          log.error(s"unexpected error processing tasks for ${offerId.getValue} from ${sender()}", e)
          data
      }

      dataWithTasks.nextConsumerOpt match {
        case Some((matcher, contData)) =>
          val contDataWithActiveMatcher =
            if (addedTasks.nonEmpty) contData.addConsumer(matcher)
            else contData
          offerQueues += offerId -> contDataWithActiveMatcher
          contDataWithActiveMatcher
        case None =>
          log.warning("Got unexpected matched tasks.")
          dataWithTasks
      }
    }

    offerQueues.get(offerId).foreach { data =>
      val nextData = processAddedTasks(data)
      scheduleNextMatcherOrFinish(nextData)
    }
  }

  private[this] def scheduleNextMatcherOrFinish(data: OfferData): Unit = {
    val nextConsumerOpt = if (data.deadline < clock.now()) {
      log.warning(s"Deadline for ${data.offer.getId.getValue} overdue. Scheduled ${data.tasks.size} tasks so far.")
      None
    }
    else if (launchTokens <= 0) {
      log.warning(s"No launch tokens left for ${data.offer.getId.getValue}. Scheduled ${data.tasks.size} tasks so far.")
      None
    }
    else {
      data.nextConsumerOpt
    }

    nextConsumerOpt match {
      case Some((nextConsumer, newData)) =>
        import context.dispatcher
        pipe(nextConsumer.processOffer(newData.deadline, newData.offer)).pipeTo(self)
      case None =>
        data.sender ! OfferMatcher.MatchedTasks(data.offer.getId, data.tasks)
        offerQueues -= data.offer.getId
        log.info(s"Finished processing ${data.offer.getId.getValue}. Matched ${data.tasks.size} tasks.")
    }
  }
}

