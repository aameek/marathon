package mesosphere.marathon.core.task.bus

import mesosphere.marathon.core.task.bus.TaskStatusObservables.TaskStatusUpdate
import mesosphere.marathon.state.{ PathId, Timestamp }
import mesosphere.marathon.tasks.TaskIdUtil
import org.joda.time.DateTime

class TaskStatusUpdateTH(val wrapped: TaskStatusUpdate) {
  def withAppId(appId: String): TaskStatusUpdateTH = TaskStatusUpdateTH {
    wrapped.copy(taskId = TaskStatusUpdateTH.newTaskID(appId))
  }
}

object TaskStatusUpdateTH {
  def apply(update: TaskStatusUpdate): TaskStatusUpdateTH =
    new TaskStatusUpdateTH(update)

  private def newTaskID(appId: String) = {
    TaskIdUtil.newTaskId(PathId(appId))
  }

  val taskId = newTaskID("/app")

  val running = TaskStatusUpdateTH(
    TaskStatusUpdate(
      timestamp = Timestamp.apply(new DateTime(2015, 2, 3, 12, 30, 0, 0)),
      taskId = taskId,
      status = MarathonTaskStatusTH.running
    )
  )

  val staging = TaskStatusUpdateTH(
    TaskStatusUpdate(
      timestamp = Timestamp.apply(new DateTime(2015, 2, 3, 12, 31, 0, 0)),
      taskId = taskId,
      status = MarathonTaskStatusTH.running
    )
  )
}
