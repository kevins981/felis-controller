package edu.toronto.felis

import io.vertx.core.AsyncResult
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import io.vertx.lang.scala.{ ScalaVerticle, VertxExecutionContext }
import io.vertx.scala.core.eventbus.Message
import io.vertx.scala.ext.web.Router
import io.vertx.scala.core.{Context, Vertx}
import io.vertx.scala.ext.web.handler.{BodyHandler, StaticHandler }
import java.io.File
import java.nio.file.{ FileSystems, Files, Path, Paths, StandardWatchEventKinds, WatchKey }
import scala.collection.mutable.MutableList

import scala.concurrent.Future
import scala.util.{Success, Failure}

object HttpVerticle {
  var HttpPort = 8666
}

class HttpVerticle extends ScalaVerticle {

  override def startFuture(): Future[_] = {
    val router = Router.router(vertx)
    val bus = vertx.eventBus()

    router
      .route().handler(BodyHandler.create())

    router
      .get("/config/")
      .handler(_.response().end(FelisVerticle.configFileBuffer))

    router
      .get("/static/*")
      .handler(
        StaticHandler.create("static")
          .setCachingEnabled(false)
          .setDirectoryListing(true))

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
      .listenFuture(HttpVerticle.HttpPort, "0.0.0.0")
  }
}

object Server {
  var vertx: Vertx = null

  def stop(): Unit = {
    vertx.close()
    Thread.sleep(1000)
  }

  def startAllServices(): Unit = {
    implicit val executionContext = VertxExecutionContext(vertx.getOrCreateContext())
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
        stop()
      }
    }
  }

  def start(): Unit = {
    // use the default config.json
    FelisVerticle.loadConfig("config.json")
    vertx = Vertx.vertx()
    startAllServices()
  }
}

object Main extends App {
  def parseArgs(): Unit = {
    if (args.length != 1) {
      println(s"missing <config file> parameter")
      System.exit(-1)
    }
    FelisVerticle.loadConfig(args(0))
  }

  // as main method
  parseArgs()
  Server.vertx = Vertx.vertx()
  Server.startAllServices()
}
