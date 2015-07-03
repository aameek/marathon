package mesosphere.marathon.core.task.bus

trait TaskBusModule {
  def taskStatusEmitter: TaskStatusEmitter
  def taskStatusObservable: TaskStatusObservables
}
