package mesosphere.marathon.core.launcher

trait LauncherModule {
  def offerProcessor: OfferProcessor
  def taskLauncher: TaskLauncher
}