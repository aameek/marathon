package mesosphere.marathon.core.launcher

import mesosphere.marathon.MarathonSchedulerDriverHolder
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.launcher.impl.DefaultLauncherModule
import mesosphere.marathon.core.matcher.OfferMatcher
import mesosphere.marathon.core.task.bus.TaskStatusEmitter

trait LauncherModule {
  def offerProcessor: OfferProcessor
  def taskLauncher: TaskLauncher
}

object LauncherModule {
  def apply(
    clock: Clock,
    marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder,
    taskStatusEmitter: TaskStatusEmitter,
    offerMatcher: OfferMatcher): LauncherModule =
    new DefaultLauncherModule(clock, marathonSchedulerDriverHolder, taskStatusEmitter, offerMatcher)
}