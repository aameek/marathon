package mesosphere.marathon.core
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.launcher.LauncherModule
import mesosphere.marathon.core.matcher.app.AppOfferMatcherModule
import mesosphere.marathon.core.task.bus.TaskBusModule

trait CoreModule {
  def clock: Clock
  def taskBusModule: TaskBusModule
  def launcherModule: LauncherModule
  def appOfferMatcherModule: AppOfferMatcherModule
}
