// -*- mode: scala -*-
import mill._, mill.modules._, scalalib._

object FelisController extends ScalaModule {
  def scalaVersion = "2.12.7"
  def ivyDeps = Agg(
    ivy"io.vertx::vertx-lang-scala:3.7.0",
    ivy"io.vertx::vertx-web-scala:3.7.0",
    ivy"${scalaOrganization()}:scala-reflect:${scalaVersion()}",
  )

  def mainClass = Some("edu.toronto.felis.Main")
}
