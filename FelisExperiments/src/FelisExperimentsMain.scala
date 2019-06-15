package edu.toronto.felis

import java.io.{FileOutputStream, FileWriter}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

class YcsbExperiment extends Experiment {
  override def boot(): Unit = {
    val args = Array(Experiment.Binary, "-c", Experiment.ControllerHost, "-n", "host1","-w", "ycsb") ++ cmdArguments()
    println(s"Making outdir ${outputDir()}")
    os.makeDir.all(os.Path(outputDir()))

    println(s"Booting with running ${args.mkString(" ")}")
    processes += os.proc(args).spawn(cwd = Experiment.WorkingDir, stderr = os.Inherit, stdout = os.Inherit)
  }
}

class YcsbPartitionExperiment(override val cpu: Int,
                              override val memory: Int,
                              override val skewFactor: Int,
                              override val contented: Boolean) extends YcsbExperiment with Contented with Skewed {
  addAttribute("partition")

  override def plotSymbol = "Partition"

  override def cmdArguments() =
    super.cmdArguments() ++ Array("-XYcsbEnablePartition", "-XEpochQueueLength100M", "-XVHandleLockElision")
}

class YcsbLockingExperiment(override val cpu: Int,
                            override val memory: Int,
                            override val skewFactor: Int,
                            override val contented: Boolean) extends YcsbExperiment with Contented with Skewed {
  addAttribute("locking")

  override def plotSymbol = "Locking"
}

object ExperimentsMain extends App {
  def run() = {
    val all = ArrayBuffer[Experiment]()
    for (i <- 0 until 5) {
      for (cpu <- Seq(8, 16, 24, 32)) {
        for (contented <- Seq(true, false)) {
          for (skewFactor <- Seq(0, 90)) {
            val mem = cpu
            all.append(new YcsbPartitionExperiment(cpu, mem, skewFactor, contented))
            all.append(new YcsbLockingExperiment(cpu, mem, skewFactor, contented))
          }
        }
      }
    }
    for (e <- all) {
      println(s"Running ${e.attributes.mkString(" + ")}")
      var done = false
      while (!done) {
        try {
          e.run()
          done = true
        } catch {
          case _: ExperimentRunException => println("Failed, re-run")
        }
        Thread.sleep(1000)
      }
    }
  }

  def plot() = {
    val a = ujson.Arr()
    for (contented <- Seq(true, false)) {
      for (skewFactor <- Seq(0, 90)) {
        a.value ++= new YcsbLockingExperiment(0, 0, skewFactor, contented).loadResults().value
        a.value ++= new YcsbPartitionExperiment(0, 0, skewFactor, contented).loadResults().value
      }
    }
    val filename =
      if (args.length < 2) "wwwroot/data.json"
      else args(1)

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
        println("Cannot write to file")
    }
  }

  if (args.length == 0 || args(0) == "run") {
    run()
  } else if (args(0) == "plot") {
    plot()
  }
}
