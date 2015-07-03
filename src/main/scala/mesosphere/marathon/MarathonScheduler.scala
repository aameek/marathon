package mesosphere.marathon

import javax.inject.{ Inject, Named }

import akka.event.EventStream
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.launcher.OfferProcessor
import mesosphere.marathon.core.task.bus.TaskStatusObservables.TaskStatusUpdate
import mesosphere.marathon.core.task.bus.{ MarathonTaskStatus, TaskStatusEmitter }
import mesosphere.marathon.event._
import mesosphere.util.state.FrameworkIdUtil
import org.apache.log4j.Logger
import org.apache.mesos.Protos._
import org.apache.mesos.{ Scheduler, SchedulerDriver }

import scala.concurrent.Future
import scala.concurrent.duration._

trait SchedulerCallbacks {
  def disconnected(): Unit
}

class MarathonScheduler @Inject() (
    @Named(EventModule.busName) eventBus: EventStream,
    clock: Clock,
    offerProcessor: OfferProcessor,
    taskStatusEmitter: TaskStatusEmitter,
    frameworkIdUtil: FrameworkIdUtil,
    config: MarathonConf,
    schedulerCallbacks: SchedulerCallbacks) extends Scheduler {

  private[this] val log = Logger.getLogger(getClass.getName)

  import mesosphere.util.ThreadPoolContext.context

  implicit val zkTimeout = config.zkTimeoutDuration

  override def registered(
    driver: SchedulerDriver,
    frameworkId: FrameworkID,
    master: MasterInfo): Unit = {
    log.info(s"Registered as ${frameworkId.getValue} to master '${master.getId}'")
    frameworkIdUtil.store(frameworkId)

    eventBus.publish(SchedulerRegisteredEvent(frameworkId.getValue, master.getHostname))
  }

  override def reregistered(driver: SchedulerDriver, master: MasterInfo): Unit = {
    log.info("Re-registered to %s".format(master))

    eventBus.publish(SchedulerReregisteredEvent(master.getHostname))
  }

  override def resourceOffers(driver: SchedulerDriver, offers: java.util.List[Offer]): Unit = {
    // Check for any tasks which were started but never entered TASK_RUNNING
    // TODO resourceOffers() doesn't feel like the right place to run this
    //    val toKill = taskTracker.checkStagedTasks
    //
    //    if (toKill.nonEmpty) {
    //      log.warn(s"There are ${toKill.size} tasks stuck in staging for more " +
    //        s"than ${config.taskLaunchTimeout()}ms which will be killed")
    //      log.info(s"About to kill these tasks: $toKill")
    //      for (task <- toKill)
    //        driver.killTask(protos.TaskID(task.getId))
    //    }

    // remove queued tasks with stale (non-current) app definition versions
    //    val appVersions: Map[PathId, Timestamp] =
    //      Await.result(appRepo.currentAppVersions(), config.zkTimeoutDuration)

    // FIXME
    //    taskQueue.retain {
    //      case QueuedTask(app, _) =>
    //        appVersions.get(app.id) contains app.version
    //    }

    val deadline = clock.now() + 1.second
    import scala.collection.JavaConverters._
    offers.asScala.foreach { offer =>
      offerProcessor.processOffer(deadline, offer)
    }
  }

  override def offerRescinded(driver: SchedulerDriver, offer: OfferID): Unit = {
    log.info("Offer %s rescinded".format(offer))
  }

  override def statusUpdate(driver: SchedulerDriver, status: TaskStatus): Unit = {
    log.info("Received status update for task %s: %s (%s)"
      .format(status.getTaskId.getValue, status.getState, status.getMessage))

    taskStatusEmitter.publish(TaskStatusUpdate(timestamp = clock.now(), status.getTaskId, MarathonTaskStatus(status)))
  }

  override def frameworkMessage(
    driver: SchedulerDriver,
    executor: ExecutorID,
    slave: SlaveID,
    message: Array[Byte]): Unit = {
    log.info("Received framework message %s %s %s ".format(executor, slave, message))
    eventBus.publish(MesosFrameworkMessageEvent(executor.getValue, slave.getValue, message))
  }

  override def disconnected(driver: SchedulerDriver) {
    log.warn("Disconnected")

    eventBus.publish(SchedulerDisconnectedEvent())

    // Disconnection from the Mesos master has occurred.
    // Thus, call the scheduler callbacks.
    schedulerCallbacks.disconnected()
  }

  override def slaveLost(driver: SchedulerDriver, slave: SlaveID) {
    log.info(s"Lost slave $slave")
  }

  override def executorLost(
    driver: SchedulerDriver,
    executor: ExecutorID,
    slave: SlaveID,
    p4: Int) {
    log.info(s"Lost executor $executor slave $p4")
  }

  override def error(driver: SchedulerDriver, message: String) {
    log.warn("Error: %s".format(message))
    suicide()
  }

  private def suicide(): Unit = {
    log.fatal("Committing suicide")

    //scalastyle:off magic.number
    // Asynchronously call sys.exit() to avoid deadlock due to the JVM shutdown hooks
    Future {
      sys.exit(9)
    } onFailure {
      case t: Throwable => log.fatal("Exception while committing suicide", t)
    }
  }
}
