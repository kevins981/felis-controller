package edu.toronto.felis

import java.io.FileWriter
import java.net.{InetAddress, SocketTimeoutException}

import com.flyberrycapital.slack.SlackClient

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.TreeMap
import scala.util.{Failure, Success, Try}

class ExperimentRunException extends Exception {}

object Experiment {
  var ControllerHost = System.getProperty("controller.host", "127.0.0.1:3148")
  var ControllerHttp = System.getProperty("controller.http", "127.0.0.1:8666")
  var Binary = "~/workspace/felis/buck-out/gen/db#release"
  var WorkingDir = "~/workspace/felis/results"
}

trait Experiment {
  val attributes = new ArrayBuffer[String]()
  protected val processes = new ArrayBuffer[os.SubProcess]()
  private var valid = true

  def cpu = 16
  def memory = 16
  def epochSize = -1
  def plotSymbol = ""

  // Helper function for adding a new process
  def spawnProcess(args: Seq[String]) = {
    println(s"Spawn Process: ${args.mkString(" ")}")
    processes += os.proc(args).spawn(cwd = os.Path.expandUser(Experiment.WorkingDir), stderr = os.Inherit, stdout = os.Inherit)
  }

  protected def boot(): Unit
  protected def kill(): Unit = {
    for (p <- processes) {
      p.destroy()
      p.destroyForcibly()
      p.close()
    }
    processes.clear()
  }
  protected def waitToFinish() = {
    for (p <- processes) {
      if (p.waitFor(5 * 60 * 1000) || p.exitCode() != 0) {
        valid = false
      }
      if (p.isAlive()) {
        println("Process timed out. Killing forcibly.")
        p.destroyForcibly()
      }
      p.close()
    }
    processes.clear()
  }
  protected def hasExited(): Boolean = {
    for (p <- processes) {
      if (!p.isAlive())
        return true
    }
    return false
  }

  protected def die() = {
    kill()
    throw new ExperimentRunException()
  }

  def run(): Unit = {
    boot()
    var ready = false

    while (!ready) {
      Thread.sleep(2000)
      if (hasExited()) die()

      // println("Polling the process")
      try {
        val r = requests.post(
          "http://%s/broadcast/".format(Experiment.ControllerHttp),
          data = "{\"type\": \"get_status\"}",
          readTimeout = 300000,
          connectTimeout = 300000)

        if (r.statusCode != 200) die()
        val status = ujson.read(r.text).arr
        ready = true
        for (machineStatus <- status) {
          if (machineStatus.obj("status").str != "listening")
            ready = false
        }
      } catch {
        case e: ExperimentRunException => throw e
        case e: requests.TimeoutException => {
          Thread.sleep(3000)
          println("Timeout, retry")
        }
        case e: Throwable => {
          e.printStackTrace()
          println("Failed, retry")
        }
      }
    }

    Thread.sleep(1000)
    println("Starting now")
    val r = requests.post(
      "http://%s/broadcast/".format(Experiment.ControllerHttp),
      data = "{\"type\": \"status_change\", \"status\": \"connecting\"}")
    if (r.statusCode != 200) die()

    waitToFinish()
    if (!isResultValid())
      die()
    clean()
  }
  def isResultValid() = valid
  def clean(): Unit = {}

  def loadResults(): ujson.Arr = {
    val result = ujson.Arr()
    for (filepath <- os.list(os.Path(outputDir()))) {
      if (filepath.last.endsWith(".json")) {
        println(s"loading ${filepath}")
        val obj = ujson.read(os.read(filepath)).obj
        obj.put("attribute", attributes.mkString("_"))
        obj.put("symbol", plotSymbol)
        obj.put("filename", filepath.last)
        result.value.append(obj)
      }
    }
    return result
  }

  def addAttribute(attr: String) = attributes.append(attr)
  def outputDir() = (os.Path.expandUser(Experiment.WorkingDir).toString +: attributes).mkString("/")
  def cmdArguments() = {
    val maxEpochs = if (epochSize > 0) 4000000 / epochSize else 40
    Array(
      "-Xcpu%02d".format(cpu),
      "-Xmem%02dG".format(memory),
      "-XOutputDir%s".format(outputDir())
    ) ++ (if (epochSize > 0) Array(s"-XEpochSize${epochSize}", s"-XNrEpoch${maxEpochs}") else Array[String]())
  }

  if (epochSize > 0) addAttribute("epoch%d".format(epochSize))
}

object ExperimentSuite {
  private val suites = TreeMap[String, ExperimentSuite]()
  private val ProgressBarWidth = 20

  private def run(name: String, desc: String, all: Seq[Experiment]) = {
    var cur = 0
    var progress = -1
    val tot = all.length
    val hostname = InetAddress.getLocalHost().getHostName()
    val slkToken = System.getProperty("slackToken")
    val slk = if (slkToken != null) Some(new SlackClient(slkToken)) else None

    val header = s"Experiments start running on ${hostname}. Total ${tot} experiments.\n${name}: ${desc}\n"
    val msg = slk.map { _.chat.postMessage("#db", header) }
    for (e <- all) {
      cur += 1
      if (ProgressBarWidth * cur / tot > progress) {
        progress += 1
        try {
          slk.map {
            _.chat.update(msg.get,
              header + s"`Progress [${"=".repeat(progress) + " ".repeat(ProgressBarWidth - progress)}] ${cur}/${tot}`")
          }
        } catch {
          case e: SocketTimeoutException => println("Slack update failed, ignoring")
        }
      }

      println(s"${hostname} Running ${e.attributes.mkString(" + ")} ${cur}/${tot}")
      try {
        e.run()
      } catch {
        case _: ExperimentRunException => {
          var again = false
          do {
            try {
              slk.map {
                _.chat.postMessage("#db", s"Experiment on ${hostname} ${e.attributes.mkString(" + ")} failed, skipping...")
              }
            } catch {
              case e: SocketTimeoutException => again = true
              case _: Throwable => {}
            }
          } while (again)

          println("Failed")
        }
      }
      Thread.sleep(1000)
    }
    Thread.sleep(1000)
    slk.map {
      _.chat.postMessage("#db", s"Experiments all done on ${hostname}.")
    }
  }

  def invoke(name: String) = {
    suites.get(name) match {
      case Some(s) => {
        val runs = ArrayBuffer[Experiment]()
        s.setup(runs)
        run(s.name, s.description, runs)
      }
      case None => {
        show()
      }
    }
  }

  def show() = {
    println("Available runs: ")
    for (k <- suites.keys) {
      println(s" run${k}: ${suites(k).description}")
    }
    println()
  }

  def apply(name: String, description: String)(setup: (ArrayBuffer[Experiment]) => Unit): Unit = {
    val s = new ExperimentSuite(name, description, setup)
    suites(s.name) = s
  }
}

case class ExperimentSuite(val name: String, val description: String, val setup: (ArrayBuffer[Experiment]) => Unit) {}

object PlotSuite {
  private val suites = TreeMap[String, (String, () => ujson.Arr)]()

  def show() = {
    println("Available plots: ")
    for (k <- suites.keys) {
      println(s" plot${k}")
    }
    println()
  }

  def invoke(name: String) = {
    suites.get(name) match {
      case Some(s) => {
        val (filename, fn) = s
        val a = fn()
        println(s"Writing to ${filename}")

        val file = Try(
          new FileWriter(filename))

        file match {
          case Success(f) => try {
            ujson.writeTo(a, f)
          } catch {
            case e: Exception => e.printStackTrace()
          } finally {
            f.close()
          }
          case Failure(_) =>
            println(s"Cannot write to file ${filename}")
        }
      }
      case None => {
        show()
      }
    }
  }

  def apply(name: String, filename: String)(fn: () => ujson.Arr) = {
    suites(name) = (filename, fn)
  }
}
