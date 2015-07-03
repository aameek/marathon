package mesosphere.marathon.core.task.bus.impl

import mesosphere.marathon.core.task.bus.{ TaskStatusUpdateTH, TaskBusModule }
import mesosphere.marathon.core.task.bus.TaskStatusObservables.TaskStatusUpdate
import mesosphere.marathon.state.PathId
import org.scalatest.{ BeforeAndAfter, FunSuite }

class DefaultTaskStatusModuleTest extends FunSuite with BeforeAndAfter {
  var module: TaskBusModule = _
  before {
    module = new DefaultTaskBusModule
  }

  test("observable forAll includes all app status updates") {
    var received = List.empty[TaskStatusUpdate]
    module.taskStatusObservable.forAll.foreach(received :+= _)
    val aa: TaskStatusUpdate = TaskStatusUpdateTH.running.withAppId("/a/a").wrapped
    val ab: TaskStatusUpdate = TaskStatusUpdateTH.running.withAppId("/a/b").wrapped
    module.taskStatusEmitter.publish(aa)
    module.taskStatusEmitter.publish(ab)
    assert(received == List(aa, ab))
  }

  test("observable forAppId includes only app status updates") {
    var received = List.empty[TaskStatusUpdate]
    module.taskStatusObservable.forAppId(PathId("/a/a")).foreach(received :+= _)
    val aa: TaskStatusUpdate = TaskStatusUpdateTH.running.withAppId("/a/a").wrapped
    val ab: TaskStatusUpdate = TaskStatusUpdateTH.running.withAppId("/a/b").wrapped
    module.taskStatusEmitter.publish(aa)
    module.taskStatusEmitter.publish(ab)
    assert(received == List(aa))
  }
}
