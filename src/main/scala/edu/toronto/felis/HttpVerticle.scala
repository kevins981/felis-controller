package edu.toronto.felis

import io.vertx.core.AsyncResult
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import io.vertx.lang.scala.{ ScalaVerticle, VertxExecutionContext }
import io.vertx.scala.core.eventbus.Message
import io.vertx.scala.ext.web.Router
import io.vertx.scala.core.{Context, Vertx}
import io.vertx.scala.ext.web.handler.BodyHandler
import java.io.File
import java.nio.file.{ FileSystems, Files, Path, Paths, StandardWatchEventKinds, WatchKey }
import scala.collection.mutable.MutableList

import scala.concurrent.Future
import scala.util.{Success, Failure}

class HttpVerticle extends ScalaVerticle {

  override def startFuture(): Future[_] = {
    val router = Router.router(vertx)
    val bus = vertx.eventBus()

    router
      .route().handler(BodyHandler.create())

    router
      .get("/hello/")
      .handler(_.response().end("Hello World!\n"))

    router
      .get("/config/")
      .handler(_.response().end(FelisVerticle.configFileBuffer))

    router
      .post("/broadcast/")
      .handler {
        ctx =>
        ctx.getBody() match {
          case Some(body) =>
            bus.send("broadcastMessage", body, {
              result: AsyncResult[Message[Buffer]] =>

              if (result.succeeded()) {
                ctx.response().end(result.result().body())
              } else {
                ctx.response().setStatusCode(500).end()
              }
            })
          case None =>
            ctx.response()
              .setStatusCode(404)
              .end()
        }
      }

    println("Starting Http Server")

    vertx
      .createHttpServer()
      .requestHandler(router.accept _)
      .listenFuture(8666, "0.0.0.0")
  }
}

object Main extends App {
  val vertx = Vertx.vertx()
  implicit val executionContext = VertxExecutionContext(vertx.getOrCreateContext())
  val watcher = FileSystems.getDefault().newWatchService()

  def shutdown(): Unit = {
    watcher.close()
    vertx.close()
  }

  def watchRecursively(dir: File): Unit = {
    if (!dir.isDirectory()) {
      return
    }

    val path = dir.toPath()
    path.register(watcher,
      StandardWatchEventKinds.ENTRY_CREATE,
      StandardWatchEventKinds.ENTRY_DELETE,
      StandardWatchEventKinds.ENTRY_MODIFY)

    for (sub <- dir.listFiles()) {
      watchRecursively(sub)
    }
  }

  def watchSourceCodeChange(dir: String): Unit = {
    watchRecursively(new File(dir))

    vertx.periodicStream(50).handler({ _ =>
      if (watcher.poll() != null) {
        println("File Changed, so exit the server.")
        println("You should use this with sbt's ~run task.")
        shutdown()
      }
    })
  }

  def startAllServices(): Unit = {
    val deployTasks = List(
      vertx.deployVerticleFuture(ScalaVerticle.nameForVerticle[HttpVerticle]),
      vertx.deployVerticleFuture(ScalaVerticle.nameForVerticle[FelisVerticle]),
    )
    Future.sequence(deployTasks).onComplete {
      case Success(s) => {
        println("Server all initialized.")
      }
      case Failure(t) => {
        t.printStackTrace()
        shutdown()
      }
    }
  }

  def parseArgs(): Unit = {
    if (args.length != 1) {
      println(s"missing <config file> parameter")
      System.exit(-1)
    }
    FelisVerticle.loadConfig(args(0))
  }

  parseArgs()
  watchSourceCodeChange("src")
  startAllServices()
}
