package mesosphere.marathon.core.matcher.app.impl

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import mesosphere.marathon.state.{ AppDefinition, PathId }
import mesosphere.marathon.tasks.TaskQueue
import mesosphere.marathon.tasks.TaskQueue.QueuedTaskCount

import scala.collection.immutable.Seq
import scala.concurrent.Await
import scala.concurrent.duration.{ Deadline, _ }
import scala.reflect.ClassTag
import scala.util.control.NonFatal

private[impl] class CoreTaskQueue(actorRef: ActorRef) extends TaskQueue {
  override def list: Seq[QueuedTaskCount] = askActor("list")(CoreTaskQueueActor.List).asInstanceOf[Seq[QueuedTaskCount]]
  override def count(appId: PathId): Int = askActor("count")(CoreTaskQueueActor.Count(appId)).asInstanceOf[Int]
  override def listApps: Seq[AppDefinition] = list.map(_.app)
  override def listWithDelay: Seq[(QueuedTaskCount, Deadline)] =
    list.map(_ -> Deadline.now) // TODO: include rateLimiter
  override def purge(appId: PathId): Unit = askActor("purge")(CoreTaskQueueActor.Purge(appId))
  override def add(app: AppDefinition, count: Int): Unit = askActor("add")(CoreTaskQueueActor.Add(app, count))

  private[this] def askActor[T](method: String)(message: T): Any = {
    implicit val timeout: Timeout = 1.second
    val answerFuture = actorRef ? message
    import scala.concurrent.ExecutionContext.Implicits.global
    answerFuture.recover {
      case NonFatal(e) => throw new RuntimeException(s"in $method", e)
    }
    Await.result(answerFuture, 1.second)
  }
}
