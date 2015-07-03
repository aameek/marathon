package mesosphere.marathon.core

import javax.inject.{ Named, Provider }

import akka.actor.ActorRef
import akka.event.EventStream
import com.google.inject.name.Names
import com.google.inject.{ AbstractModule, Inject, Provides, Singleton }
import mesosphere.marathon.MarathonSchedulerDriverHolder
import mesosphere.marathon.core.CoreGuiceModule.TaskStatusUpdateActorProvider
import mesosphere.marathon.core.base.actors.{ ActorsModule, DefaultActorsModule }
import mesosphere.marathon.core.base.{ ClockModule, DefaultClockModule, DefaultRandomModule, DefaultShutdownHookModule, RandomModule, ShutdownHookModule }
import mesosphere.marathon.core.launcher.impl.DefaultLauncherModule
import mesosphere.marathon.core.launcher.{ LauncherModule, OfferProcessor }
import mesosphere.marathon.core.matcher.OfferMatcherModule
import mesosphere.marathon.core.matcher.app.AppOfferMatcherModule
import mesosphere.marathon.core.matcher.app.impl.DefaultAppOfferMatcherModule
import mesosphere.marathon.core.matcher.impl.DefaultOfferMatcherModule
import mesosphere.marathon.core.task.bus.impl.DefaultTaskBusModule
import mesosphere.marathon.core.task.bus.{ TaskBusModule, TaskStatusEmitter, TaskStatusObservable }
import mesosphere.marathon.core.task.tracker.TaskStatusUpdateActor
import mesosphere.marathon.event.EventModule
import mesosphere.marathon.health.HealthCheckManager
import mesosphere.marathon.tasks.{ TaskFactory, TaskIdUtil, TaskQueue, TaskTracker }

/**
  * Provides the glue between guice and the core modules.
  */
class CoreGuiceModule extends AbstractModule {

  // Export classes used outside of core to guice
  @Provides @Singleton
  def clock = clockModule.clock
  @Provides @Singleton
  def offerProcessor(launcherModule: LauncherModule): OfferProcessor = launcherModule.offerProcessor
  @Provides @Singleton
  def taskStatusEmitter: TaskStatusEmitter = taskBusModule.taskStatusEmitter
  @Provides @Singleton
  def taskStatusObservable: TaskStatusObservable = taskBusModule.taskStatusObservable
  @Provides @Singleton
  final def taskQueue(appOfferMatcherModule: AppOfferMatcherModule): TaskQueue = appOfferMatcherModule.taskQueue

  override def configure(): Unit = {
    // Start on startup
    bind(classOf[ActorRef])
      .annotatedWith(Names.named("taskStatusUpdateActor"))
      .toProvider(classOf[TaskStatusUpdateActorProvider])
      .asEagerSingleton()
  }

  // private to core module

  @Provides @Singleton
  def clockModule_ = clockModule
  lazy val clockModule: ClockModule = new DefaultClockModule()
  lazy val randomModule: RandomModule = new DefaultRandomModule()
  lazy val shutdownHookModule: ShutdownHookModule = new DefaultShutdownHookModule()

  @Provides @Singleton
  def taskBusModule_ = taskBusModule
  lazy val taskBusModule: TaskBusModule = new DefaultTaskBusModule()

  @Provides @Singleton
  def actorsModule_ = actorsModule
  lazy val actorsModule: ActorsModule = new DefaultActorsModule(shutdownHookModule)

  @Provides @Singleton
  def offerMatcherModule_ = offerMatcherModule
  lazy val offerMatcherModule: OfferMatcherModule =
    new DefaultOfferMatcherModule(taskBusModule, clockModule, randomModule, actorsModule)

  @Provides @Singleton
  def launcherModule(
    marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder,
    offerMatcherModule: OfferMatcherModule): LauncherModule =
    new DefaultLauncherModule(
      clockModule, taskBusModule,
      marathonSchedulerDriverHolder, offerMatcherModule
    )

  @Provides @Singleton
  def appOfferMatcherModule(
    actorsModule: ActorsModule,
    clockModule: ClockModule,
    offerMatcherModule: OfferMatcherModule,
    taskBusModule: TaskBusModule,
    taskTracker: TaskTracker,
    taskFactory: TaskFactory): AppOfferMatcherModule = {

    new DefaultAppOfferMatcherModule(
      actorsModule,
      clockModule,
      offerMatcherModule,
      taskBusModule,
      taskTracker,
      taskFactory)
  }

}

object CoreGuiceModule {
  class TaskStatusUpdateActorProvider @Inject() (
      actorsModule: ActorsModule,
      taskStatusObservable: TaskStatusObservable,
      @Named(EventModule.busName) eventBus: EventStream,
      @Named("schedulerActor") schedulerActor: ActorRef,
      taskIdUtil: TaskIdUtil,
      healthCheckManager: HealthCheckManager,
      taskTracker: TaskTracker,
      marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder) extends Provider[ActorRef] {

    override def get(): ActorRef = {
      val props = TaskStatusUpdateActor.props(
        taskStatusObservable, eventBus, schedulerActor, taskIdUtil, healthCheckManager, taskTracker,
        marathonSchedulerDriverHolder
      )
      println("XXXXX" +
        "\nXXXXXXXX\nXXXXX")
      actorsModule.actorSystem.actorOf(props, "taskStatusUpdate")
    }
  }
}
