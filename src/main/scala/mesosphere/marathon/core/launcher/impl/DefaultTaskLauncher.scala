package mesosphere.marathon.core.launcher.impl

import java.util.Collections

import mesosphere.marathon.MarathonSchedulerDriverHolder
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.launcher.TaskLauncher
import mesosphere.marathon.core.task.bus.{ MarathonTaskStatus, TaskStatusEmitter }
import mesosphere.marathon.core.task.bus.TaskStatusObservables.TaskStatusUpdate
import org.apache.mesos.Protos.{ OfferID, TaskInfo }
import org.apache.mesos.SchedulerDriver
import org.slf4j.LoggerFactory

private[impl] class DefaultTaskLauncher(
  marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder,
  clock: Clock,
  taskStatusEmitter: TaskStatusEmitter)
    extends TaskLauncher {
  private[this] val log = LoggerFactory.getLogger(getClass)

  override def launchTasks(offerID: OfferID, taskInfos: Seq[TaskInfo]): Unit = {
    marathonSchedulerDriverHolder.driver match {
      case Some(driver) =>
        import scala.collection.JavaConverters._
        driver.launchTasks(Collections.singleton(offerID), taskInfos.asJava)
        taskInfos.foreach { taskInfo =>
          taskStatusEmitter.publish(
            TaskStatusUpdate(clock.now(), taskInfo.getTaskId, MarathonTaskStatus.LaunchRequested)
          )
        }
      case None =>
        taskInfos.foreach { taskInfo =>
          log.warn("Cannot launch tasks because no driver is available")
          taskStatusEmitter.publish(
            TaskStatusUpdate(clock.now(), taskInfo.getTaskId, MarathonTaskStatus.LaunchDenied)
          )
        }
    }
  }

  override def declineOffer(offerID: OfferID): Unit = {
    withDriver(s"declineOffer(${offerID.getValue})") {
      _.declineOffer(offerID)
    }
  }

  private[this] def withDriver(description: => String)(block: SchedulerDriver => Unit) = {
    marathonSchedulerDriverHolder.driver match {
      case Some(driver) => block(driver)
      case None         => log.warn(s"Cannot execute '$description', no driver available")
    }
  }
}
