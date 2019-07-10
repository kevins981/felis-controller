package edu.toronto.felis

import scala.collection.mutable.ArrayBuffer

class ExperimentRunException extends Exception {}

object Experiment {
  var ControllerHost = "127.0.0.1:3148"
  var ControllerHttp = "127.0.0.1:8666"
  var Binary = "~/workspace/felis/buck-out/gen/db#release"
  var WorkingDir = "~/workspace/felis/results"
}

trait Experiment {
  val attributes = new ArrayBuffer[String]()
  protected val processes = new ArrayBuffer[os.SubProcess]()
  private var valid = true

  def cpu = 16
  def memory = 16
  def plotSymbol = ""

  protected def boot(): Unit
  protected def kill(): Unit = {
    for (p <- processes) {
      p.destroy()
      p.destroyForcibly()
    }
  }
  protected def waitToFinish() = {
    for (p <- processes) {
      p.waitFor()
      if (p.exitCode() != 0) {
        valid = false
      }
      p.close()
    }
  }
  protected def hasExited(): Boolean = {
    for (p <- processes) {
      if (!p.isAlive())
        return true
    }
    return false
  }

  private def die() = {
    kill()
    throw new ExperimentRunException()
  }

  def run(): Unit = {
    boot()
    var ready = false

    while (!ready) {
      Thread.sleep(3000)
      if (hasExited()) die()

      // println("Polling the process")
      val r = requests.post(
        "http://%s/broadcast/".format(Experiment.ControllerHttp),
        data = "{\"type\": \"get_status\"}")
      if (r.statusCode != 200) die()
      val status = ujson.read(r.text).arr
      ready = true
      for (machineStatus <- status) {
        if (machineStatus.obj("status").str != "listening")
          ready = false
      }
    }

    Thread.sleep(5000)
    println("Starting now")
    val r = requests.post(
      "http://%s/broadcast/".format(Experiment.ControllerHttp),
      data = "{\"type\": \"status_change\", \"status\": \"connecting\"}")
    if (r.statusCode != 200) die()

    waitToFinish()
    if (!isResultValid())
      die()
  }
  def isResultValid() = valid

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
  def cmdArguments() = Array(
    "-Xcpu%02d".format(cpu),
    "-Xmem%02dG".format(memory),
    "-XOutputDir%s".format(outputDir())
  )
}

// Configurations in the experiments
trait Contented extends Experiment {
  def contented = false
  addAttribute(if (contented) "contention" else "nocontention")

  override def cmdArguments(): Array[String] = {
    val extra = if (!contented) Array("-XYcsbReadOnly8", "-XYcsbTableSize1000000") else Array[String]()
    super.cmdArguments() ++ extra
  }
}

trait Skewed extends Experiment {
  def skewFactor = 0
  addAttribute(if (skewFactor == 0) "noskew" else "skew%02d".format(skewFactor))

  override def cmdArguments(): Array[String] = {
    val extra = if (skewFactor > 0) Array("-XYcsbSkewFactor%02d".format(skewFactor)) else Array[String]()
    super.cmdArguments() ++ extra
  }
}
