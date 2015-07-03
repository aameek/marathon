package mesosphere.marathon.core.task.tracker

import javax.inject.Named

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.event.{ LoggingReceive, EventStream }
import mesosphere.marathon.MarathonSchedulerActor.ScaleApp
import mesosphere.marathon.MarathonSchedulerDriverHolder
import mesosphere.marathon.Protos.MarathonTask
import mesosphere.marathon.core.task.bus.MarathonTaskStatus.WithMesosStatus
import mesosphere.marathon.core.task.bus.TaskStatusObservables
import mesosphere.marathon.core.task.bus.TaskStatusObservables.TaskStatusUpdate
import mesosphere.marathon.event.{ EventModule, MesosStatusUpdateEvent }
import mesosphere.marathon.health.HealthCheckManager
import mesosphere.marathon.state.Timestamp
import mesosphere.marathon.tasks.{ TaskIdUtil, TaskTracker }
import org.apache.mesos.Protos.TaskStatus
import rx.lang.scala.Subscription

import scala.util.{ Failure, Success }

private[core] object TaskStatusUpdateActor {
  def props(
    taskStatusObservable: TaskStatusObservables,
    @Named(EventModule.busName) eventBus: EventStream,
    @Named("schedulerActor") schedulerActor: ActorRef,
    taskIdUtil: TaskIdUtil,
    healthCheckManager: HealthCheckManager,
    taskTracker: TaskTracker,
    marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder): Props = {

    Props(new TaskStatusUpdateActor(
      taskStatusObservable,
      eventBus,
      schedulerActor,
      taskIdUtil,
      healthCheckManager,
      taskTracker,
      marathonSchedulerDriverHolder))
  }
}

/**
  * Processes task status update events, mostly to update the task tracker.
  *
  * FIXME:
  *   How can we make sure that we do not miss events on startup?
  *   => This has to be tied to task reconciliation somehow.
  */
private class TaskStatusUpdateActor(
  taskStatusObservable: TaskStatusObservables,
  @Named(EventModule.busName) eventBus: EventStream,
  @Named("schedulerActor") schedulerActor: ActorRef,
  taskIdUtil: TaskIdUtil,
  healthCheckManager: HealthCheckManager,
  taskTracker: TaskTracker,
  marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder)
    extends Actor with ActorLogging {
  var taskStatusUpdateSubscription: Subscription = _

  override def preStart(): Unit = {
    super.preStart()

    log.info("Starting")
    taskStatusUpdateSubscription = taskStatusObservable.forAll.subscribe(self ! _)
  }

  override def postStop(): Unit = {
    super.postStop()

    taskStatusUpdateSubscription.unsubscribe()
    log.info("Stopped")
  }

  //TODO: fix style issue and enable this scalastyle check
  //scalastyle:off cyclomatic.complexity method.length
  override def receive: Receive = LoggingReceive {
    case TaskStatusUpdate(timestamp, taskId, WithMesosStatus(status)) =>
      val appId = taskIdUtil.appId(taskId)

      // forward health changes to the health check manager
      val maybeTask = taskTracker.fetchTask(appId, taskId.getValue)
      for (marathonTask <- maybeTask)
        healthCheckManager.update(status, Timestamp(marathonTask.getVersion))

      import org.apache.mesos.Protos.TaskState._
      import context.dispatcher
      status.getState match {
        case TASK_ERROR | TASK_FAILED | TASK_FINISHED | TASK_KILLED | TASK_LOST =>
          // Remove from our internal list
          taskTracker.terminated(appId, status).foreach { taskOption =>
            taskOption match {
              case Some(task) => postEvent(status, task)
              case None       => log.warning(s"Couldn't post event for ${status.getTaskId}")
            }

            schedulerActor ! ScaleApp(appId)
          }

        case TASK_RUNNING if !maybeTask.exists(_.hasStartedAt) => // staged, not running
          taskTracker.running(appId, status).onComplete {
            case Success(task) =>
              // FIXME(PK): Incldue that in new
              //            appRepo.app(appId, Timestamp(task.getVersion)).onSuccess {
              //              case maybeApp: Option[AppDefinition] => maybeApp.foreach(taskQueue.rateLimiter.resetDelay)
              //            }
              postEvent(status, task)

            case Failure(t) =>
              log.warning(s"Couldn't post event for ${status.getTaskId}", t)
              log.warning(s"Killing task ${status.getTaskId}")
              driverOpt.foreach(_.killTask(status.getTaskId))
          }

        case TASK_STAGING if !taskTracker.contains(appId) =>
          log.warning(s"Received status update for unknown app $appId")
          log.warning(s"Killing task ${status.getTaskId}")
          driverOpt.foreach(_.killTask(status.getTaskId))

        case _ =>
          taskTracker.statusUpdate(appId, status).onSuccess {
            case None =>
              log.warning(s"Killing task ${status.getTaskId}")
              driverOpt.foreach(_.killTask(status.getTaskId))
          }
      }
  }

  private[this] def driverOpt = marathonSchedulerDriverHolder.driver

  private[this] def postEvent(status: TaskStatus, task: MarathonTask): Unit = {
    log.info("Sending event notification.")
    import scala.collection.JavaConverters._
    eventBus.publish(
      MesosStatusUpdateEvent(
        status.getSlaveId.getValue,
        status.getTaskId.getValue,
        status.getState.name,
        if (status.hasMessage) status.getMessage else "",
        taskIdUtil.appId(status.getTaskId),
        task.getHost,
        task.getPortsList.asScala,
        task.getVersion
      )
    )
  }

}
