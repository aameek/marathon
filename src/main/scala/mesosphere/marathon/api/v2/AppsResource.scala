package mesosphere.marathon.api.v2

import java.net.URI
import java.util.UUID
import javax.inject.{ Inject, Named }
import javax.servlet.http.HttpServletRequest
import javax.ws.rs._
import javax.ws.rs.core.{ Context, MediaType, Response }

import akka.event.EventStream
import com.codahale.metrics.annotation.Timed
import mesosphere.marathon.api.v2.json.EnrichedTask
import mesosphere.marathon.api.v2.json.Formats._
import mesosphere.marathon.api.{ BeanValidation, ModelValidation, RestResource }
import mesosphere.marathon.event.{ ApiPostEvent, EventModule }
import mesosphere.marathon.health.{ HealthCheckManager, HealthCounts }
import mesosphere.marathon.state.PathId._
import mesosphere.marathon.state._
import mesosphere.marathon.tasks.TaskTracker
import mesosphere.marathon.upgrade.{ DeploymentPlan, DeploymentStep, RestartApplication }
import mesosphere.marathon.{ ConflictingChangeException, MarathonConf, MarathonSchedulerService, UnknownAppException }
import play.api.libs.json.{ JsObject, Json, Writes }

import scala.collection.immutable.Seq
import scala.concurrent.Future

@Path("v2/apps")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class AppsResource @Inject() (
    @Named(EventModule.busName) eventBus: EventStream,
    service: MarathonSchedulerService,
    taskTracker: TaskTracker,
    healthCheckManager: HealthCheckManager,
    taskFailureRepository: TaskFailureRepository,
    val config: MarathonConf,
    groupManager: GroupManager) extends RestResource {

  import AppsResource._
  import mesosphere.util.ThreadPoolContext.context

  val ListApps = """^((?:.+/)|)\*$""".r
  val EmbedTasks = "apps.tasks"
  val EmbedTasksAndFailures = "apps.failures"

  @GET
  @Timed
  def index(@QueryParam("cmd") cmd: String,
            @QueryParam("id") id: String,
            @QueryParam("label") label: String,
            @QueryParam("embed") embed: String): String = {
    val apps = search(Option(cmd), Option(id), Option(label))
    val runningDeployments = result(service.listRunningDeployments()).map(r => r._1)
    val mapped = embed match {
      case EmbedTasks =>
        apps.map { app =>
          val enrichedApp = app.withTasksAndDeployments(
            enrichedTasks(app),
            healthCounts(app),
            runningDeployments
          )
          WithTasksAndDeploymentsWrites.writes(enrichedApp)
        }

      case EmbedTasksAndFailures =>
        apps.map { app =>
          WithTasksAndDeploymentsAndFailuresWrites.writes(
            app.withTasksAndDeploymentsAndFailures(
              enrichedTasks(app),
              healthCounts(app),
              runningDeployments,
              taskFailureRepository.current(app.id)
            )
          )
        }

      case _ =>
        apps.map { app =>
          val enrichedApp = app.withTaskCountsAndDeployments(
            enrichedTasks(app),
            healthCounts(app),
            runningDeployments
          )
          WithTaskCountsAndDeploymentsWrites.writes(enrichedApp)
        }
    }

    Json.obj("apps" -> mapped).toString()
  }

  @POST
  @Timed
  def create(@Context req: HttpServletRequest, body: Array[Byte],
             @DefaultValue("false")@QueryParam("force") force: Boolean): Response = {

    val app = validateApp(Json.parse(body).as[AppDefinition].withCanonizedIds())

    def createOrThrow(opt: Option[AppDefinition]) = opt
      .map(_ => throw new ConflictingChangeException(s"An app with id [${app.id}] already exists."))
      .getOrElse(app)

    val plan = result(groupManager.updateApp(app.id, createOrThrow, app.version, force))

    val managedAppWithDeployments = app.withTasksAndDeployments(
      appTasks = Nil,
      healthCounts = HealthCounts(0, 0, 0),
      runningDeployments = Seq(plan)
    )

    maybePostEvent(req, app)
    Response.created(new URI(app.id.toString)).entity(managedAppWithDeployments).build()
  }

  @GET
  @Path("""{id:.+}""")
  @Timed
  def show(@PathParam("id") id: String): Response = {
    def runningDeployments: Seq[DeploymentPlan] = result(service.listRunningDeployments()).map(r => r._1)
    def transitiveApps(gid: PathId): Response = {
      val apps = result(groupManager.group(gid)).map(group => group.transitiveApps).getOrElse(Nil)
      val withTasks = apps.map { app =>
        val enrichedApp = app.withTasksAndDeploymentsAndFailures(
          enrichedTasks(app),
          healthCounts(app),
          runningDeployments,
          taskFailureRepository.current(app.id)
        )

        WithTasksAndDeploymentsAndFailuresWrites.writes(enrichedApp)
      }
      ok(Json.obj("*" -> withTasks).toString())
    }
    def app(): Future[Response] = groupManager.app(id.toRootPath).map {
      case Some(app) =>
        val mapped = app.withTasksAndDeploymentsAndFailures(
          enrichedTasks(app),
          healthCounts(app),
          runningDeployments,
          taskFailureRepository.current(app.id)
        )
        ok(Json.obj("app" -> WithTasksAndDeploymentsAndFailuresWrites.writes(mapped)).toString())

      case None => unknownApp(id.toRootPath)
    }
    id match {
      case ListApps(gid) => transitiveApps(gid.toRootPath)
      case _             => result(app())
    }
  }

  @PUT
  @Path("""{id:.+}""")
  @Timed
  def replace(@Context req: HttpServletRequest,
              @PathParam("id") id: String,
              @DefaultValue("false")@QueryParam("force") force: Boolean,
              body: Array[Byte]): Response = {
    val appUpdate = Json.parse(body).as[AppUpdate]
    // prefer the id from the AppUpdate over that in the UI
    val appId = appUpdate.id.map(_.canonicalPath()).getOrElse(id.toRootPath)
    // TODO error if they're not the same?
    val updateWithId = appUpdate.copy(id = Some(appId))
    BeanValidation.requireValid(ModelValidation.checkUpdate(updateWithId, needsId = false))

    def createApp() = validateApp(appUpdate(AppDefinition(appId)))
    def updateApp(current: AppDefinition) = validateApp(appUpdate(current))
    def rollback(version: Timestamp) = service.getApp(appId, version).getOrElse(throw new UnknownAppException(appId))
    def updateOrRollback(current: AppDefinition) = updateWithId.version.map(rollback).getOrElse(updateApp(current))
    def updateOrCreate(opt: Option[AppDefinition]) = opt.map(updateOrRollback).getOrElse(createApp())
    val newVersion = Timestamp.now()
    val plan = result(groupManager.updateApp(appId, updateOrCreate(_).copy(version = newVersion), newVersion, force))

    val response = plan.original.app(appId).map(_ => Response.ok()).getOrElse(Response.created(new URI(appId.toString)))
    maybePostEvent(req, plan.target.app(appId).get)
    deploymentResult(plan, response)
  }

  @PUT
  @Timed
  def replaceMultiple(@DefaultValue("false")@QueryParam("force") force: Boolean, body: Array[Byte]): Response = {
    val updates = Json.parse(body).as[Seq[AppUpdate]].map(_.withCanonizedIds())
    BeanValidation.requireValid(ModelValidation.checkUpdates(updates))
    val version = Timestamp.now()
    def updateApp(id: PathId, update: AppUpdate, appOption: Option[AppDefinition]): AppDefinition = {
      val currentOrNew = appOption.getOrElse(AppDefinition(id))
      update.version.flatMap(v => service.getApp(id, v)).getOrElse(validateApp(update(currentOrNew)))
    }
    def updateGroup(root: Group): Group = updates.foldLeft(root) { (group, update) =>
      update.id match {
        case Some(id) => group.updateApp(id, updateApp(id, update, _), version)
        case None     => group
      }
    }
    val deployment = result(groupManager.update(PathId.empty, updateGroup, version, force))
    deploymentResult(deployment)
  }

  @DELETE
  @Path("""{id:.+}""")
  @Timed
  def delete(@Context req: HttpServletRequest,
             @DefaultValue("true")@QueryParam("force") force: Boolean,
             @PathParam("id") id: String): Response = {
    val appId = id.toRootPath

    def deleteApp(group: Group) = group.app(appId)
      .map(_ => group.removeApplication(appId))
      .getOrElse(throw new UnknownAppException(appId))

    deploymentResult(result(groupManager.update(appId.parent, deleteApp, force = force)))
  }

  @Path("{appId:.+}/tasks")
  def appTasksResource(): AppTasksResource =
    new AppTasksResource(service, taskTracker, healthCheckManager, config, groupManager)

  @Path("{appId:.+}/versions")
  def appVersionsResource(): AppVersionsResource = new AppVersionsResource(service, config)

  @POST
  @Path("{id:.+}/restart")
  def restart(@PathParam("id") id: String,
              @DefaultValue("false")@QueryParam("force") force: Boolean): Response = {
    val appId = id.toRootPath
    val newVersion = Timestamp.now()
    def setVersionOrThrow(opt: Option[AppDefinition]) = opt
      .map(_.copy(version = newVersion))
      .getOrElse(throw new UnknownAppException(appId))

    def restartApp(versionChange: DeploymentPlan): DeploymentPlan = {
      val newApp = versionChange.target.app(appId).get
      val plan = DeploymentPlan(
        UUID.randomUUID().toString,
        versionChange.original,
        versionChange.target,
        DeploymentStep(RestartApplication(newApp) :: Nil) :: Nil,
        Timestamp.now())
      result(service.deploy(plan, force = force))
      plan
    }
    //this will create an empty deployment, since version chances do not trigger restarts
    val versionChange = result(groupManager.updateApp(id.toRootPath, setVersionOrThrow, newVersion, force))
    //create a restart app deployment plan manually
    deploymentResult(restartApp(versionChange))
  }

  private def validateApp(app: AppDefinition): AppDefinition = {
    BeanValidation.requireValid(ModelValidation.checkAppConstraints(app, app.id.parent))
    val conflicts = ModelValidation.checkAppConflicts(app, app.id.parent, service)
    if (conflicts.nonEmpty) throw new ConflictingChangeException(conflicts.mkString(","))
    app
  }

  private def enrichedTasks(app: AppDefinition): Seq[EnrichedTask] = {
    val tasks = taskTracker.get(app.id).map { task =>
      task.getId -> task
    }.toMap

    for {
      (taskId, results) <- result(healthCheckManager.statuses(app.id)).to[Seq]
      task <- tasks.get(taskId)
    } yield EnrichedTask(app.id, task, results)
  }

  private def healthCounts(app: AppDefinition): HealthCounts = result(healthCheckManager.healthCounts(app.id))

  private def maybePostEvent(req: HttpServletRequest, app: AppDefinition) =
    eventBus.publish(ApiPostEvent(req.getRemoteAddr, req.getRequestURI, app))

  private[v2] def search(cmd: Option[String], id: Option[String], label: Option[String]): Iterable[AppDefinition] = {
    def containCaseInsensitive(a: String, b: String): Boolean = b.toLowerCase contains a.toLowerCase
    val selectors = label.map(new LabelSelectorParsers().parsed)

    service.listApps().filter { app =>
      val appMatchesCmd = cmd.fold(true)(c => app.cmd.exists(containCaseInsensitive(c, _)))
      val appMatchesId = id.fold(true)(s => containCaseInsensitive(s, app.id.toString))
      val appMatchesLabel = selectors.fold(true)(_.matches(app))
      appMatchesCmd && appMatchesId && appMatchesLabel
    }
  }
}

object AppsResource {
  implicit val WithTaskCountsAndDeploymentsWrites: Writes[AppDefinition.WithTaskCountsAndDeployments] = Writes { app =>
    val appJson = AppDefinitionWrites.writes(app).as[JsObject]

    appJson ++ Json.obj(
      "tasksStaged" -> app.tasksStaged,
      "tasksRunning" -> app.tasksRunning,
      "tasksHealthy" -> app.tasksHealthy,
      "tasksUnhealthy" -> app.tasksUnhealthy,
      "deployments" -> app.deployments
    )
  }

  implicit val WithTasksAndDeploymentsWrites: Writes[AppDefinition.WithTasksAndDeployments] = Writes { app =>
    val appJson = WithTaskCountsAndDeploymentsWrites.writes(app).as[JsObject]

    appJson ++ Json.obj(
      "tasks" -> app.tasks
    )
  }

  implicit val WithTasksAndDeploymentsAndFailuresWrites: Writes[AppDefinition.WithTasksAndDeploymentsAndTaskFailures] =
    Writes { app =>
      val appJson = WithTasksAndDeploymentsWrites.writes(app).as[JsObject]

      appJson ++ Json.obj(
        "lastTaskFailure" -> app.lastTaskFailure
      )
    }
}
