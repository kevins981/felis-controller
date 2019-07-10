package edu.toronto.felis

import java.io.{FileOutputStream, FileWriter}

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.{Failure, Success, Try}

class YcsbExperiment extends Experiment {
  override def boot(): Unit = {
    val args = Array(os.Path.expandUser(Experiment.Binary).toString,
      "-c", Experiment.ControllerHost,
      "-n", "host1",
      "-w", "ycsb") ++ cmdArguments()

    println(s"Making outdir ${outputDir()}")
    os.makeDir.all(os.Path.expandUser(outputDir()))

    println(s"Booting with running ${args.mkString(" ")}")
    processes += os.proc(args).spawn(cwd = os.Path.expandUser(Experiment.WorkingDir), stderr = os.Inherit, stdout = os.Inherit)
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

class TpccExperiment(val nodes: Int) extends Experiment {
  override def boot() = {
    println(s"Making outdir ${outputDir()}")
    os.makeDir.all(os.Path.expandUser(outputDir()))
    val warehouses = nodes * cpu
    println(s"Total number of warehouses ${warehouses}")

    for (i <- 1 to nodes) {
      val nodeName = s"host${i}"
      val args = Array(Experiment.Binary, "-c", Experiment.ControllerHost, "-n", nodeName, "-w", "tpcc",
        "-XEpochQueueLength1m", "-XNrEpoch40", s"-XMaxNodeLimit${nodes}", s"-XTpccWarehouses${warehouses}") ++ cmdArguments()

      launchProcess(nodeName, args)
    }
  }

  def launchProcess(nodeName: String, args: Seq[String]) = {
    println(s"Launching process with ${args.mkString(" ")}")
  }
}

object MultiNodeTpccExperiment {
  val HostnameMapping = HashMap[String, String]()
  val Formatter = new java.text.SimpleDateFormat("yyyy-MM-dd-HH:mm:ss")

  def getHostnameMapping(): Unit = {
    val r = requests.get(s"http://${Experiment.ControllerHttp}/config/")
    if (r.statusCode != 200) throw new ExperimentRunException()
    val hostsJson = ujson.read(r.data.bytes.filter(p => p != 0)).obj
    val nodesJson = hostsJson("nodes").arr
    for (nodeJson <- nodesJson) {
      val name = nodeJson("name").str
      val hostname = nodeJson("ssh_hostname").str
      HostnameMapping += ((name, hostname))
    }
    println(HostnameMapping)
  }
  getHostnameMapping()
}

class MultiNodeTpccExperiment(override val nodes: Int) extends TpccExperiment(nodes) {
  addAttribute(s"multi${nodes}")
  override def cpu = 16
  override def memory = 18
  override def plotSymbol = "TPC-C"
  override def launchProcess(nodeName: String, args: Seq[String]) = {
    super.launchProcess(nodeName, args)
    val sshAgent = sys.env("SSH_AUTH_SOCK")
    if (sshAgent == null) {
      throw new ExperimentRunException()
    } else {
      println(s"Using ssh agent at ${sshAgent}")
    }
    val sshHost = MultiNodeTpccExperiment.HostnameMapping(nodeName)
    val procArgs = Seq("ssh", sshHost) ++ args
    println(s"Spawning ${procArgs.mkString(" ")}")
    processes += os.proc(procArgs).spawn(
      stdout = os.pwd / s"${nodeName}.out", stderr = os.pwd / s"${nodeName}.err")
    Thread.sleep(1000)
  }
  override def loadResults(): ujson.Arr = {
    val rs = super.loadResults()
    val perNode = ArrayBuffer[ArrayBuffer[(Long, ujson.Obj)]]()
    0 until nodes foreach { _ =>
      perNode += ArrayBuffer[(Long, ujson.Obj)]()
    }
    val agg = ujson.Arr()
    for (o <- rs.value) {
      val fn = o("filename").str
      if (!fn.endsWith(".json")) throw new ExperimentRunException()
      val afn = fn.dropRight(5).split("-")
      val nodeNr = afn(0).drop(4).toInt
      val date = MultiNodeTpccExperiment.Formatter.parse(afn(1))
      perNode(nodeNr - 1) += ((date.getTime, o.obj))
    }
    var len = 0
    perNode foreach { a =>
      a.sortBy(_._1)
      if (len > 0 && a.length != len) throw new ExperimentRunException()
      len = a.length
    }
    0 until len foreach { _ =>
    }
    agg
  }
}

object ExperimentsMain extends App {
  def run(all: Seq[Experiment]) = {
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

  def runYcsb() = {
    val all = ArrayBuffer[Experiment]()
    0 until 5 foreach { _ =>
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
    run(all)
  }

  def runMultiTpcc() = {
    val all = ArrayBuffer[Experiment]()
    0 until 10 foreach { _ =>
      1 to 4 foreach { i =>
        all += new MultiNodeTpccExperiment(i)
      }
    }
    run(all)
  }

  def plotTo(filename: String)(generateFn: (ujson.Arr) => ujson.Arr) = {
    val a = generateFn(ujson.Arr())
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

  def plotYcsb() = {
    plotTo("static/ycsb.json") { a =>
      for (contented <- Seq(true, false)) {
        for (skewFactor <- Seq(0, 90)) {
          a.value ++= new YcsbLockingExperiment(0, 0, skewFactor, contented).loadResults().value
          a.value ++= new YcsbPartitionExperiment(0, 0, skewFactor, contented).loadResults().value
        }
      }
      a
    }
  }

  def plotMultiTpcc() = {
    // TODO:
  }

  if (args.length == 0) {
    println("runYcsb|runMultiTpcc|plotYcsb|plotMultiTpcc")
    sys.exit(-1)
  }

  if (args(0) == "runYcsb") {
    runYcsb()
  } else if (args(0) == "plotYcsb") {
    plotYcsb()
  } else if (args(0) == "runMultiTpcc") {
    Experiment.ControllerHttp = "142.150.234.2:8666"
    Experiment.ControllerHost = "142.150.234.2:3148"
    runMultiTpcc()
  } else {
    println("mismatch")
  }

  sys.exit(0)
}
