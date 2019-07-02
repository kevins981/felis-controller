// -*- mode: scala -*-
import mill._
import mill.scalalib._

object FelisController extends ScalaModule {
  def scalaVersion = "2.12.7"
  override def ivyDeps = Agg(
    ivy"io.vertx::vertx-lang-scala:3.7.0",
    ivy"io.vertx::vertx-web-scala:3.7.0",
    ivy"${scalaOrganization()}:scala-reflect:${scalaVersion()}",
  )
  override def mainClass = Some("edu.toronto.felis.Main")
}

object FelisExperiments extends ScalaModule {
  def scalaVersion = "2.12.7"
  override def forkArgs = Seq("")

  override def ivyDeps = Agg(
    ivy"com.lihaoyi::os-lib:0.3.0",
    ivy"com.lihaoyi::requests:0.1.8",
    ivy"com.lihaoyi::upickle:0.7.1",
  )
  override def mainClass = Some("edu.toronto.felis.ExperimentsMain")
}
