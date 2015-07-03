package mesosphere.marathon.core.matcher.app.impl

import akka.actor.{ Props, ActorRef }
import mesosphere.marathon.core.base.ClockModule
import mesosphere.marathon.core.base.actors.ActorsModule
import mesosphere.marathon.core.matcher.OfferMatcherModule
import mesosphere.marathon.core.matcher.app.AppOfferMatcherModule
import mesosphere.marathon.core.task.bus.TaskBusModule
import mesosphere.marathon.state.AppDefinition
import mesosphere.marathon.tasks.{ TaskFactory, TaskTracker, TaskQueue }

private[core] class DefaultAppOfferMatcherModule(
    actorsModule: ActorsModule,
    clockModule: ClockModule,
    offerMatcherModule: OfferMatcherModule,
    taskBusModule: TaskBusModule,
    taskTracker: TaskTracker,
    taskFactory: TaskFactory) extends AppOfferMatcherModule {

  override lazy val taskQueue: TaskQueue = new CoreTaskQueue(taskQueueActorRef)

  private[this] def appActorProps(app: AppDefinition, count: Int): Props =
    AppTaskLauncherActor.props(
      offerMatcherModule.subOfferMatcherManager,
      clockModule.clock,
      taskFactory,
      taskBusModule.taskStatusObservable,
      taskTracker)(app, count)

  private[this] lazy val taskQueueActorRef: ActorRef = {
    val props = CoreTaskQueueActor.props(appActorProps)
    actorsModule.actorSystem.actorOf(props, "taskQueue")
  }

}
