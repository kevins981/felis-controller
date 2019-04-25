import mill._, mill.modules._, scalalib._
import java.net.URLClassLoader

object ServerState {
  // if loader is null, then meaning the server is not running
  var loader: URLClassLoader = null
  var mainClassName: String = null
  var classPath: Agg[os.Path] = null

  private def runWithNewClassLoader(cl: ClassLoader, func: ClassLoader => Unit) = {
    val currentThread = Thread.currentThread()
    val oldCl = currentThread.getContextClassLoader
    currentThread.setContextClassLoader(cl)
    try {
      func(cl)
    } finally {
      currentThread.setContextClassLoader(oldCl)
    }
  }

  def start()(implicit ctx: mill.api.Ctx.Home): Unit = {
    stop()
    loader = mill.api.ClassLoader.create(classPath.map(_.toIO.toURI.toURL).toVector, null)

    runWithNewClassLoader(
      loader,
      (cl: ClassLoader) => {
        val ins = cl.loadClass(mainClassName + "$").getField("MODULE$").get(null)
        cl.loadClass(mainClassName).getMethod("start").invoke(ins)
      })
  }

  def stop()(implicit ctx: mill.api.Ctx.Home): Unit = {
    if (loader == null) {
      println("loader is null, nothing to clean up")
      return
    }
    println("stopping...")
    runWithNewClassLoader(
      loader,
      (cl: ClassLoader) => {
        val ins = cl.loadClass(mainClassName + "$").getField("MODULE$").get(null)
        cl.loadClass(mainClassName).getMethod("stop").invoke(ins)
      })

    loader.close()
    loader = null
  }
}

object FelisController extends ScalaModule {
  def scalaVersion = "2.12.5"
  def ivyDeps = Agg(
    ivy"io.vertx::vertx-lang-scala:3.5.2",
    ivy"io.vertx::vertx-web-scala:3.5.2",
    ivy"${scalaOrganization()}:scala-reflect:${scalaVersion()}",
  )

  def mainClass = Some("edu.toronto.felis.Main")

  def serverState = T.worker {
    ServerState.mainClassName = "edu.toronto.felis.Server"
    ServerState.classPath = runClasspath().map(_.path)

    ServerState
  }

  def startServer() = T.command {
    serverState().start()
  }

  def stopServer() = T.command {
    serverState().stop()
  }
}
