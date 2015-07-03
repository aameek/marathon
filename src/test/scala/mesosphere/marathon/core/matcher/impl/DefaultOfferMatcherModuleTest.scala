package mesosphere.marathon.core.matcher.impl

import mesosphere.marathon.MarathonTestHelper
import mesosphere.marathon.core.base.actors.ActorsModule
import mesosphere.marathon.core.base.{ Clock, ShutdownHooks }
import mesosphere.marathon.core.matcher.OfferMatcher.MatchedTasks
import mesosphere.marathon.core.matcher.{ OfferMatcher, OfferMatcherModule }
import mesosphere.marathon.core.task.bus.TaskBusModule
import mesosphere.marathon.state.Timestamp
import org.apache.mesos.Protos.{ CommandInfo, Offer, SlaveID, TaskID, TaskInfo }
import org.scalatest.{ BeforeAndAfter, FunSuite }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }
import scala.util.Random

class DefaultOfferMatcherModuleTest extends FunSuite with BeforeAndAfter {
  test("no registered matchers result in empty result") {
    val offer: Offer = MarathonTestHelper.makeBasicOffer().build()
    val matchedTasksFuture: Future[MatchedTasks] =
      module.offerMatcher.processOffer(clock.now() + 1.second, offer)
    val matchedTasks: MatchedTasks = Await.result(matchedTasksFuture, 3.seconds)
    assert(matchedTasks.tasks.isEmpty)
  }

  test("single offer is passed to matcher") {
    val offer: Offer = MarathonTestHelper.makeBasicOffer().build()

    val task = newTask("task1")
    val subMatchedTasks = MatchedTasks(offer.getId, Seq(task))
    module.subOfferMatcherManager.addOfferMatcher(new ConstantOfferMatcher(subMatchedTasks))

    val matchedTasksFuture: Future[MatchedTasks] =
      module.offerMatcher.processOffer(clock.now() + 1.second, offer)
    val matchedTasks: MatchedTasks = Await.result(matchedTasksFuture, 3.seconds)
    assert(matchedTasks.tasks == Seq(task))
  }

  test("single offer is passed to multiple matchers") {
    val offer: Offer = MarathonTestHelper.makeBasicOffer().build()

    val task = newTask("task1")
    val subMatchedTasks = MatchedTasks(offer.getId, Seq(task))
    module.subOfferMatcherManager.addOfferMatcher(new ConstantOfferMatcher(subMatchedTasks))

    val matchedTasksFuture: Future[MatchedTasks] =
      module.offerMatcher.processOffer(clock.now() + 1.second, offer)
    val matchedTasks: MatchedTasks = Await.result(matchedTasksFuture, 3.seconds)
    assert(matchedTasks.tasks == Seq(task))
  }

  private def newTask(taskId: String): TaskInfo = {
    TaskInfo.newBuilder()
      .setName("true")
      .setTaskId(TaskID.newBuilder().setValue(taskId).build())
      .setSlaveId(SlaveID.newBuilder().setValue("slave1").build())
      .setCommand(CommandInfo.newBuilder().setShell(true).addArguments("true"))
      .build()
  }

  private[this] var module: OfferMatcherModule = _
  private[this] var shutdownHookModule: ShutdownHooks = _
  private[this] var clock: Clock = _

  before {
    shutdownHookModule = ShutdownHooks()
    clock = Clock()
    val random = Random
    val actorSystem = ActorsModule(shutdownHookModule).actorSystem
    val taskBusModule = TaskBusModule()
    module = OfferMatcherModule(
      clock, random, actorSystem, taskBusModule.taskStatusEmitter)
  }

  after {
    shutdownHookModule.shutdown()
  }

  private class ConstantOfferMatcher(matchedTasks: MatchedTasks) extends OfferMatcher {
    override def processOffer(deadline: Timestamp, offer: Offer): Future[MatchedTasks] = Future.successful(matchedTasks)
  }
}
