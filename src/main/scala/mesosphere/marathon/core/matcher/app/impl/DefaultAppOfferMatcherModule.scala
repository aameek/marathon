package mesosphere.marathon.core.matcher.app.impl

import akka.actor.{ ActorRef, ActorSystem, Props }
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.matcher.OfferMatcherManager
import mesosphere.marathon.core.matcher.app.AppOfferMatcherModule
import mesosphere.marathon.core.task.bus.TaskStatusObservables
import mesosphere.marathon.state.AppDefinition
import mesosphere.marathon.tasks.{ TaskFactory, TaskQueue, TaskTracker }

private[core] class DefaultAppOfferMatcherModule(
    actorSystem: ActorSystem,
    clock: Clock,
    subOfferMatcherManager: OfferMatcherManager,
    taskStatusObservables: TaskStatusObservables,
    taskTracker: TaskTracker,
    taskFactory: TaskFactory) extends AppOfferMatcherModule {

  override lazy val taskQueue: TaskQueue = new CoreTaskQueue(taskQueueActorRef)

  private[this] def appActorProps(app: AppDefinition, count: Int): Props =
    AppTaskLauncherActor.props(
      subOfferMatcherManager,
      clock,
      taskFactory,
      taskStatusObservables,
      taskTracker)(app, count)

  private[this] lazy val taskQueueActorRef: ActorRef = {
    val props = CoreTaskQueueActor.props(appActorProps)
    actorSystem.actorOf(props, "taskQueue")
  }

}
