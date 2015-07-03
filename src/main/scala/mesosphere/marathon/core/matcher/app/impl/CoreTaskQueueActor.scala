package mesosphere.marathon.core.matcher.app.impl

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{
  PoisonPill,
  Terminated,
  Actor,
  ActorLogging,
  ActorRef,
  OneForOneStrategy,
  Props,
  SupervisorStrategy
}
import akka.event.LoggingReceive
import akka.pattern.{ ask, pipe }
import akka.util.Timeout
import mesosphere.marathon.state.{ AppDefinition, PathId }
import mesosphere.marathon.tasks.TaskQueue.QueuedTaskCount

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

private[impl] object CoreTaskQueueActor {
  def props(appActorProps: (AppDefinition, Int) => Props): Props = {
    Props(new CoreTaskQueueActor(appActorProps))
  }

  // messages to the core task actor which correspond directly to the TaskQueue interface
  sealed trait Request
  case object List extends Request
  case class Count(appId: PathId) extends Request
  case class Purge(appId: PathId) extends Request
  case class PurgeActor(actorRef: ActorRef) extends Request
  case object ConfirmPurge extends Request
  case class Add(app: AppDefinition, count: Int) extends Request

}

private[impl] class CoreTaskQueueActor(appActorProps: (AppDefinition, Int) => Props) extends Actor with ActorLogging {
  import CoreTaskQueueActor._

  // ensure unique names for children actors
  var childSerial = 0

  var appTaskLaunchers = Map.empty[PathId, ActorRef]
  var appTaskLauncherRefs = Map.empty[ActorRef, PathId]

  case class DeferredMessage(sender: ActorRef, message: Any)
  var dyingAppTaskLaunchersMessages = Map.empty[ActorRef, Vector[DeferredMessage]].withDefault(_ => Vector.empty)

  implicit val askTimeout: Timeout = 3.seconds

  override def receive: Receive = LoggingReceive {
    Seq(
      receiveHandlePurging,
      receiveHandleNormalCommands
    ).reduce(_.orElse[Any, Unit](_))
  }

  private[this] def receiveHandlePurging: Receive = {
    case Purge(appId) =>
      appTaskLaunchers.get(appId).foreach { actorRef =>
        val deferredMessages: Vector[DeferredMessage] =
          dyingAppTaskLaunchersMessages(actorRef) :+ DeferredMessage(sender(), ConfirmPurge)
        dyingAppTaskLaunchersMessages += actorRef -> deferredMessages
        actorRef ! PoisonPill
      }

    case PurgeActor(actorRef) =>
      // FIXME: Race
      appTaskLauncherRefs.get(actorRef).foreach(self ! Purge(_))

    case ConfirmPurge => sender() ! (())

    case Terminated(actorRef) =>
      appTaskLauncherRefs.get(actorRef) match {
        case Some(pathId) =>
          appTaskLauncherRefs -= actorRef
          appTaskLaunchers -= pathId

          dyingAppTaskLaunchersMessages.get(actorRef) match {
            case None                   => log.warning("Got unexpected terminated for app {}: {}", pathId, actorRef)
            case Some(deferredMessages) => deferredMessages.foreach(msg => self.tell(msg.message, msg.sender))
          }
        case None =>
          log.warning("Don't know anything about terminated actor: {}", actorRef)
      }

    case msg @ Count(appId) if appTaskLaunchers.get(appId).exists(dyingAppTaskLaunchersMessages.contains) =>
      val actorRef = appTaskLaunchers(appId)
      val deferredMessages: Vector[DeferredMessage] =
        dyingAppTaskLaunchersMessages(actorRef) :+ DeferredMessage(sender(), msg)
      dyingAppTaskLaunchersMessages += actorRef -> deferredMessages

    case msg @ Add(app, count) if appTaskLaunchers.get(app.id).exists(dyingAppTaskLaunchersMessages.contains) =>
      val actorRef = appTaskLaunchers(app.id)
      val deferredMessages: Vector[DeferredMessage] =
        dyingAppTaskLaunchersMessages(actorRef) :+ DeferredMessage(sender(), msg)
      dyingAppTaskLaunchersMessages += actorRef -> deferredMessages
  }

  private[this] def receiveHandleNormalCommands: Receive = {
    case List =>
      import context.dispatcher
      val scatter = appTaskLaunchers
        .values.filter(!dyingAppTaskLaunchersMessages.contains(_))
        .map(ref => (ref ? AppTaskLauncherActor.GetCount).mapTo[QueuedTaskCount])
      val gather: Future[Iterable[QueuedTaskCount]] = Future.sequence(scatter)
      gather.pipeTo(sender())

    case Count(appId) =>
      import context.dispatcher
      appTaskLaunchers.get(appId) match {
        case Some(actorRef) =>
          val eventualCount: Future[QueuedTaskCount] =
            (actorRef ? AppTaskLauncherActor.GetCount).mapTo[QueuedTaskCount]
          eventualCount.map(_.count).pipeTo(sender())
        case None => sender() ! 0
      }

    case Add(app, count) =>
      // TODO: verify version?
      appTaskLaunchers.get(app.id) match {
        case None =>
          import context.dispatcher
          val actorRef = createAppTaskLauncher(app, count)
          val eventualCount: Future[QueuedTaskCount] =
            (actorRef ? AppTaskLauncherActor.GetCount).mapTo[QueuedTaskCount]
          eventualCount.map(_ => ()).pipeTo(sender())

        case Some(actorRef) =>
          import context.dispatcher
          val eventualCount: Future[QueuedTaskCount] =
            (actorRef ? AppTaskLauncherActor.AddTasks(app, count)).mapTo[QueuedTaskCount]
          eventualCount.map(_ => ()).pipeTo(sender())
      }
  }

  private[this] def createAppTaskLauncher(app: AppDefinition, initialCount: Int): ActorRef = {
    val actorRef = context.actorOf(appActorProps(app, initialCount), s"$childSerial-${app.id.safePath}")
    childSerial += 1
    appTaskLaunchers += app.id -> actorRef
    appTaskLauncherRefs += actorRef -> app.id
    context.watch(actorRef)
    actorRef
  }

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case NonFatal(e) =>
      // We periodically check if scaling is needed, so we should recover. TODO: Speedup
      // Just restarting an AppTaskLauncherActor will potentially lead to starting too many tasks.
      Stop
    case m => SupervisorStrategy.defaultDecider(m)
  }
}
