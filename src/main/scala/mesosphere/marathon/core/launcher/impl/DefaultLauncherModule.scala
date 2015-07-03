package mesosphere.marathon.core.launcher.impl

import mesosphere.marathon.MarathonSchedulerDriverHolder
import mesosphere.marathon.core.base.ClockModule
import mesosphere.marathon.core.launcher.{ OfferProcessor, TaskLauncher, LauncherModule }
import mesosphere.marathon.core.matcher.OfferMatcherModule
import mesosphere.marathon.core.task.bus.TaskBusModule

private[core] class DefaultLauncherModule(
  clockModule: ClockModule,
  taskBusModule: TaskBusModule,
  marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder,
  offerMatcherModule: OfferMatcherModule)
    extends LauncherModule {

  override lazy val offerProcessor: OfferProcessor =
    new DefaultOfferProcessor(offerMatcherModule.offerMatcher, taskLauncher)

  override lazy val taskLauncher: TaskLauncher = new DefaultTaskLauncher(
    marathonSchedulerDriverHolder,
    clockModule.clock,
    taskBusModule.taskStatusEmitter)
}
