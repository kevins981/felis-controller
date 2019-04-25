package edu.toronto.felis

import io.vertx.core.buffer.Buffer
import io.vertx.core.json.{ DecodeException, JsonArray, JsonObject }
import io.vertx.lang.scala.{ ScalaVerticle, VertxExecutionContext }
import io.vertx.core.Handler
import io.vertx.scala.core.eventbus.Message
import io.vertx.scala.core.net.NetSocket
import java.nio.file.{ Files, Paths }
import java.util.ArrayList
import scala.collection.mutable.{ ArrayBuffer, MutableList, Queue }
import scala.concurrent.{ Await, ExecutionContext, Future, Promise }
import scala.concurrent.duration._
import scala.util.{ Failure, Success }

class FelisNode(val sock: NetSocket, implicit val executionContext: ExecutionContext) extends Handler[Buffer] {
  sock.handler(this)

  private var promises = Queue[Promise[JsonObject]]()
  private var parseBuffer = Buffer.buffer()
  override def handle(buffer: Buffer): Unit = {
    var last = -1
    for (i <- 0 until buffer.length()) {
      if (buffer.getByte(i) == 0) {
        parseBuffer.appendBuffer(buffer, last + 1, i - last - 1)
        val p = promises.dequeue()
        try {
          p.success(parseBuffer.toJsonObject())
        } catch {
          case e: DecodeException => {
            p.failure(e)
          }
        }
        parseBuffer = Buffer.buffer()
        last = i
      }
    }
    if (last + 1 < buffer.length())
      parseBuffer.appendBuffer(buffer, last + 1, buffer.length() - last - 1)
  }

  def sendRequest(buffer: Buffer): Future[JsonObject] = {
    val p = Promise[JsonObject]
    promises.enqueue(p)
    sock.write(buffer.appendByte(0))

    p.future
  }

  def sendConfigFile(): Unit = {
    sendRequest(FelisVerticle.configFileBuffer).onComplete {
      case Success(obj) => {
        println(obj)
      }
      case Failure(err) => {
        err.printStackTrace()
      }}
  }
}

object FelisVerticle {
  var configFileBuffer: Buffer = _
  var RpcPort = 3144
  def loadConfig(configFile: String): Unit = {
    val loadBuffer = Buffer.buffer(Files.readAllBytes(Paths.get(configFile)))
    configFileBuffer = loadBuffer.toJsonObject().toBuffer()
    val config = configFileBuffer.toJsonObject()
    val httpPort = config.getJsonObject("controller").getInteger("http_port")
    RpcPort = config.getJsonObject("controller").getInteger("rpc_port")
    println(s"Listening on $RpcPort")
    println(s"Browser access on $httpPort")
    HttpVerticle.HttpPort = httpPort
  }
}

class FelisVerticle extends ScalaVerticle with Handler[NetSocket] {
  val nodes = ArrayBuffer[FelisNode]()
  override def handle(sock: NetSocket): Unit = {
    val node = new FelisNode(sock, executionContext)
    val addr = sock.remoteAddress()
    val nodeHost = addr.host()
    val nodePort = addr.port()

    println(s"Felis node $nodeHost:$nodePort connected.")

    nodes += node
    node.sendConfigFile()

    sock.closeHandler {
      _ =>

      println(s"Felis node $nodeHost:$nodePort disconnected.")
      nodes -= node
    }
  }

  def broadcastMessage(buffer: Buffer): Future[Buffer] = {
    val p = Promise[Buffer]

    val array = new JsonArray()
    val nrNodes = nodes.size
    for (futureObj <- nodes.map(_.sendRequest(buffer))) {
      futureObj.onComplete {
        case Success(obj) =>
          array.add(obj)
          if (array.size() == nrNodes)
            p.success(array.toBuffer())
        case Failure(t) =>
          p.failure(t)
      }
    }

    if (nrNodes == 0)
      p.success(array.toBuffer())

    return p.future
  }

  override def startFuture(): Future[_] = {
    if (FelisVerticle.configFileBuffer == null) {
      println("Cannot continue without a configuration file")
      System.exit(-1)
    }
    println("Starting Felis Server")

    val bus = vertx.eventBus()
    bus.consumer("broadcastMessage", {
      message: Message[Buffer] =>

      broadcastMessage(message.body()).onComplete {
        case Success(buf) => message.reply(buf)
        case Failure(t) => println("Error")
      }
    })

    vertx
      .createNetServer()
      .connectHandler(this)
      .listenFuture(FelisVerticle.RpcPort, "0.0.0.0")
  }
}

