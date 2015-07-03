package mesosphere.marathon.core.matcher.app.impl

import akka.actor.{ ActorLogging, Actor, Props }
import akka.event.LoggingReceive
import mesosphere.marathon.Protos.MarathonTask
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.matcher.OfferMatcher.MatchedTasks
import mesosphere.marathon.core.matcher.app.impl.AppTaskLauncherActor.TimeoutTaskLaunch
import mesosphere.marathon.core.matcher.{ OfferMatcher, OfferMatcherManager }
import mesosphere.marathon.core.matcher.util.ActorOfferMatcher
import mesosphere.marathon.core.task.bus.TaskStatusObservable.TaskStatusUpdate
import mesosphere.marathon.core.task.bus.{ MarathonTaskStatus, TaskStatusObservable }
import mesosphere.marathon.state.AppDefinition
import mesosphere.marathon.tasks.TaskFactory.CreatedTask
import mesosphere.marathon.tasks.TaskQueue.QueuedTaskCount
import mesosphere.marathon.tasks.{ TaskFactory, TaskTracker }
import org.apache.mesos.Protos.TaskID
import rx.lang.scala.Subscription
import scala.concurrent.duration._

private[impl] object AppTaskLauncherActor {
  def props(
    offerMatcherManager: OfferMatcherManager,
    clock: Clock,
    taskFactory: TaskFactory,
    taskStatusObservable: TaskStatusObservable,
    taskTracker: TaskTracker)(
      app: AppDefinition,
      initialCount: Int): Props = {
    Props(new AppTaskLauncherActor(
      offerMatcherManager,
      clock, taskFactory, taskStatusObservable, taskTracker, app, initialCount))
  }

  sealed trait Requests

  /**
    * Increase the task count of the receiver.
    * The actor responds with a [[QueuedTaskCount]] message.
    */
  case class AddTasks(count: Int) extends Requests
  /**
    * Get the current count.
    * The actor responds with a [[QueuedTaskCount]] message.
    */
  case object GetCount extends Requests

  case class TimeoutTaskLaunch(taskID: TaskID) extends Requests
}

/**
  * Allows processing offers for starting tasks for the given app.
  */
private class AppTaskLauncherActor(
  offerMatcherManager: OfferMatcherManager,
  clock: Clock,
  taskFactory: TaskFactory,
  taskStatusObservable: TaskStatusObservable,
  taskTracker: TaskTracker,
  app: AppDefinition,
  initialCount: Int)
    extends Actor with ActorLogging {

  var count = initialCount
  var inFlightTaskLaunches = Set.empty[TaskID]

  var taskStatusUpdateSubscription: Subscription = _

  var runningTasks: Set[MarathonTask] = _
  var runningTasksMap: Map[String, MarathonTask] = _

  var myselfAsOfferMatcher: OfferMatcher = _

  override def preStart(): Unit = {
    super.preStart()

    log.info("Started appTaskLaunchActor for {} version {} with initial count {}", app.id, app.version, initialCount)

    myselfAsOfferMatcher = new ActorOfferMatcher(clock, self)
    offerMatcherManager.addOfferMatcher(myselfAsOfferMatcher)(context.dispatcher)
    taskStatusUpdateSubscription = taskStatusObservable.forAppId(app.id).subscribe(self ! _)
    runningTasks = taskTracker.get(app.id)
    runningTasksMap = runningTasks.map(task => task.getId -> task).toMap
  }

  override def postStop(): Unit = {
    taskStatusUpdateSubscription.unsubscribe()
    offerMatcherManager.removeOfferMatcher(myselfAsOfferMatcher)(context.dispatcher)

    super.postStop()

    log.info("Stopped appTaskLaunchActor for {} version {}", app.id, app.version)
  }

  override def receive: Receive = LoggingReceive {
    Seq(
      receiveTaskStatusUpdate,
      receiveGetCurrentCount,
      receiveAddCount,
      receiveProcessOffers
    ).reduce(_.orElse[Any, Unit](_))
  }

  private[this] def receiveTaskStatusUpdate: Receive = {
    case TaskStatusUpdate(_, taskId, MarathonTaskStatus.Terminal(_)) =>
      runningTasksMap.get(taskId.getValue).foreach { marathonTask =>
        runningTasksMap -= taskId.getValue
        runningTasks -= marathonTask
      }
    case TaskStatusUpdate(_, taskId, MarathonTaskStatus.LaunchRequested) =>
      log.debug("Task launch for {} was requested, {} task launches remain unconfirmed",
        taskId, inFlightTaskLaunches.size)
      inFlightTaskLaunches -= taskId
      if (count <= 0 && inFlightTaskLaunches.isEmpty) {
        // do not stop myself to prevent race condition
        context.parent ! CoreTaskQueueActor.PurgeActor(self)
      }

    case TaskStatusUpdate(_, taskId, MarathonTaskStatus.LaunchDenied) =>
      log.debug("Task launch for {} was denied, rescheduling, {} task launches remain unconfirmed",
        taskId, inFlightTaskLaunches.size)
      inFlightTaskLaunches -= taskId
      count += 1

    case TimeoutTaskLaunch(taskId) =>
      if (inFlightTaskLaunches.contains(taskId)) {
        log.warning("Did not receive confirmation or denial for task launch of {}", taskId)
        inFlightTaskLaunches -= taskId
      }
  }

  private[this] def receiveGetCurrentCount: Receive = {
    case AppTaskLauncherActor.GetCount => sender() ! QueuedTaskCount(app, count)
  }

  private[this] def receiveAddCount: Receive = {
    case AppTaskLauncherActor.AddTasks(addCount) =>
      count += addCount
      sender() ! QueuedTaskCount(app, count)
  }

  private[this] def receiveProcessOffers: Receive = {
    case ActorOfferMatcher.MatchOffer(deadline, offer) if clock.now() > deadline || count <= 0 =>
      sender ! MatchedTasks(offer.getId, Seq.empty)

    case ActorOfferMatcher.MatchOffer(deadline, offer) =>
      val tasks = taskFactory.newTask(app, offer, runningTasks) match {
        case Some(CreatedTask(mesosTask, marathonTask)) =>
          taskTracker.created(app.id, marathonTask)
          runningTasks += marathonTask
          runningTasksMap += marathonTask.getId -> marathonTask
          inFlightTaskLaunches += mesosTask.getTaskId
          count -= 1
          import context.dispatcher
          context.system.scheduler.scheduleOnce(3.seconds, self, TimeoutTaskLaunch(mesosTask.getTaskId))
          Seq(mesosTask)
        case None => Seq.empty
      }
      sender ! MatchedTasks(offer.getId, tasks)
  }
}
