package mesosphere.marathon.core.matcher.app

import akka.actor.ActorSystem
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.matcher.OfferMatcherManager
import mesosphere.marathon.core.matcher.app.impl.DefaultAppOfferMatcherModule
import mesosphere.marathon.core.task.bus.TaskStatusObservables
import mesosphere.marathon.tasks.{ TaskFactory, TaskTracker, TaskQueue }

private[core] trait AppOfferMatcherModule {
  def taskQueue: TaskQueue
}

object AppOfferMatcherModule {
  def apply(
    actorSystem: ActorSystem,
    clock: Clock,
    subOfferMatcherManager: OfferMatcherManager,
    taskStatusObservables: TaskStatusObservables,
    taskTracker: TaskTracker,
    taskFactory: TaskFactory): AppOfferMatcherModule = new DefaultAppOfferMatcherModule(
    actorSystem, clock,
    subOfferMatcherManager,
    taskStatusObservables,
    taskTracker,
    taskFactory
  )
}