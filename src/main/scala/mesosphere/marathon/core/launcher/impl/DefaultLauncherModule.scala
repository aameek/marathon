package mesosphere.marathon.core.launcher.impl

import mesosphere.marathon.MarathonSchedulerDriverHolder
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.launcher.{ LauncherModule, OfferProcessor, TaskLauncher }
import mesosphere.marathon.core.matcher.OfferMatcher
import mesosphere.marathon.core.task.bus.TaskStatusEmitter

private[launcher] class DefaultLauncherModule(
  clock: Clock,
  marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder,
  taskStatusEmitter: TaskStatusEmitter,
  offerMatcher: OfferMatcher)
    extends LauncherModule {

  override lazy val offerProcessor: OfferProcessor =
    new DefaultOfferProcessor(offerMatcher, taskLauncher)

  override lazy val taskLauncher: TaskLauncher = new DefaultTaskLauncher(
    marathonSchedulerDriverHolder,
    clock,
    taskStatusEmitter)
}
