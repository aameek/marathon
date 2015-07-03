package mesosphere.marathon.core.matcher.impl

import akka.actor.{ ActorRef, ActorSystem }
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.matcher.util.ActorOfferMatcher
import mesosphere.marathon.core.matcher.{ OfferMatcher, OfferMatcherManager, OfferMatcherModule }
import mesosphere.marathon.core.task.bus.TaskStatusEmitter

import scala.concurrent.duration._
import scala.util.Random

private[matcher] class DefaultOfferMatcherModule(
  clock: Clock,
  random: Random,
  actorSystem: ActorSystem,
  taskStatusEmitter: TaskStatusEmitter)
    extends OfferMatcherModule {

  private[this] lazy val offerMatcherMultiplexer: ActorRef = {
    val props = OfferMatcherMultiplexerActor.props(
      random,
      clock,
      taskStatusEmitter)
    val actorRef = actorSystem.actorOf(props, "OfferMatcherMultiplexer")
    implicit val dispatcher = actorSystem.dispatcher
    actorSystem.scheduler.schedule(
      0.seconds, DefaultOfferMatcherModule.launchTokenInterval, actorRef,
      OfferMatcherMultiplexerActor.SetTaskLaunchTokens(DefaultOfferMatcherModule.launchTokensPerInterval))
    actorRef
  }

  override val offerMatcher: OfferMatcher = new ActorOfferMatcher(clock, offerMatcherMultiplexer)

  override val subOfferMatcherManager: OfferMatcherManager = new ActorOfferMatcherManager(offerMatcherMultiplexer)
}

private[impl] object DefaultOfferMatcherModule {
  val launchTokenInterval: FiniteDuration = 30.seconds
  val launchTokensPerInterval: Int = 1000
}
