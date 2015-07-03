package mesosphere.marathon.core.matcher

import akka.actor.ActorSystem
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.matcher.impl.DefaultOfferMatcherModule
import mesosphere.marathon.core.task.bus.TaskStatusEmitter

import scala.util.Random

trait OfferMatcherModule {
  def offerMatcher: OfferMatcher
  def subOfferMatcherManager: OfferMatcherManager
}

object OfferMatcherModule {
  def apply(
    clock: Clock,
    random: Random,
    actorSystem: ActorSystem,
    taskStatusEmitter: TaskStatusEmitter): OfferMatcherModule =
    new DefaultOfferMatcherModule(clock, random, actorSystem, taskStatusEmitter)
}

