package edu.toronto.felis

import scala.collection.mutable.ArrayBuffer

class ExperimentRunException extends Exception {}

object Experiment {
  var ControllerHost = "127.0.0.1:3148"
  var ControllerHttp = "127.0.0.1:8666"
  var Binary = os.Path.expandUser("~/workspace/felis/buck-out/gen/db#release").toString()
  var WorkingDir = os.Path.expandUser("~/workspace/felis/")
}

trait Experiment {
  val attributes = new ArrayBuffer[String]()
  protected val processes = new ArrayBuffer[os.SubProcess]()
  private var valid = true

  def cpu = 16
  def memory = 16

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
      if (p.exitCode() != 0)
        valid = false
    }
  }

  def run(): Unit = {
    boot()
    Thread.sleep(60 * 1000)
    val r = requests.post("http://%s/broadcast/".format(Experiment.ControllerHttp),
      data = "{\"type\": \"status_change\", \"status\": \"connecting\"}")
    if (r.statusCode != 200) {
      kill()
      throw new ExperimentRunException()
    }
    waitToFinish()
  }
  def isResultValid() = valid

  def loadAllResults(): Unit
  def addAttribute(attr: String) = attributes.append(attr)
  def cmdArguments() = Array("-Xcpu%02d".format(cpu), "-Xmem%02dG".format(memory))
}

// Configurations in the experiments
trait Contented extends Experiment {
  def contented = false
  addAttribute(if (contented) "contention" else "nocontention")

  override def cmdArguments(): Array[String] = {
    val extra = if (!contented) Array("-XYcsbReadOnly8", "-XYcsbTableSize100000") else Array[String]()
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
