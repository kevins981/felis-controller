package edu.toronto.felis

import java.io.{FileOutputStream, FileWriter}

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.{Failure, Success, Try}

// Configurations in the experiments
trait YcsbContended extends Experiment {
  def contentionLevel = 0
  addAttribute(s"cont${contentionLevel}")

  override def cmdArguments(): Array[String] = {
    val extra = if (contentionLevel == 0) Array("-XYcsbReadOnly8") else Array(s"-XYcsbContentionKey${contentionLevel}")
    super.cmdArguments() ++ extra
  }
}

trait YcsbSkewed extends Experiment {
  def skewFactor = 0
  addAttribute(if (skewFactor == 0) "noskew" else "skew%02d".format(skewFactor))

  override def cmdArguments(): Array[String] = {
    val extra = if (skewFactor > 0) Array("-XYcsbSkewFactor%02d".format(skewFactor)) else Array[String]()
    super.cmdArguments() ++ extra
  }
}

class YcsbExperimentConfig(
  val cpu: Int,
  val memory: Int,
  val skewFactor: Int,
  val contentionLevel: Int,
  val dependency: Boolean = false)
{}

abstract class YcsbExperiment extends Experiment with YcsbContended with YcsbSkewed {
  override def boot(): Unit = {
    val args = Array(os.Path.expandUser(Experiment.Binary).toString,
      "-c", Experiment.ControllerHost,
      "-n", "host1",
      "-w", "ycsb") ++ cmdArguments()

    println(s"Making outdir ${outputDir()}")
    os.makeDir.all(os.Path.expandUser(outputDir()))

    println(s"Booting with running ${args.mkString(" ")}")
    spawnProcess(args)
  }

  implicit val config: YcsbExperimentConfig

  override def cpu = config.cpu
  override def memory = config.memory
  override def skewFactor = config.skewFactor
  override def contentionLevel = config.contentionLevel

  override def cmdArguments() =
    if (config.dependency) super.cmdArguments() ++ Array("-XYcsbDependency") else super.cmdArguments()
}

class YcsbGranolaExperiment(implicit val config: YcsbExperimentConfig) extends YcsbExperiment {
  addAttribute("granola")

  override def plotSymbol = "Granola"

  override def cmdArguments() =
    super.cmdArguments() ++ Array("-XYcsbEnablePartition", "-XEpochQueueLength100M", "-XEnableGranola")
}

class YcsbLockingExperiment(implicit val config: YcsbExperimentConfig) extends YcsbExperiment {
  addAttribute("locking")

  override def plotSymbol = "Locking"
}

class YcsbCaracalSerialExperiment(implicit val config: YcsbExperimentConfig) extends YcsbExperiment {
  addAttribute("caracal-serial")

  override def plotSymbol = "Caracal with Serial Code"

  override def cmdArguments() =
    super.cmdArguments() ++ Array("-XVHandleBatchAppend", "-XCoreScaling10") // TODO: tuneable level?
}

class YcsbCaracalPieceExperiment(implicit val config: YcsbExperimentConfig) extends YcsbExperiment {
  addAttribute("caracal-pieces")

  override def plotSymbol = "Caracal with Callback API"

  override def cmdArguments() =
    super.cmdArguments() ++ Array("-XVHandleBatchAppend", "-XVHandleParallel10") // TODO: tuneable level?
}

// TODO: These TPCC runs need a lot of renovation.

class BaseTpccExperiment(val nodes: Int) extends Experiment {
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

class HotspotTpccExperiment extends BaseTpccExperiment(1) {
  def hotspotLoad = 0

  addAttribute("hotspot%03d".format(hotspotLoad))

  override def launchProcess(nodeName: String, args: Seq[String]): Unit = {
    super.launchProcess(nodeName, args)
    spawnProcess(Seq(os.Path.expandUser(Experiment.Binary).toString) ++ args.drop(1)) // Ignoring the nodeName because this is a single node experiment
  }
  override def cmdArguments() = {
    if (hotspotLoad > 0) {
      super.cmdArguments() ++ Array("-XTpccHotWarehouseBitmap1", "-XTpccHotWarehouseLoad%03d".format(hotspotLoad))
    } else {
      super.cmdArguments()
    }
  }
}

class HotspotTpccCongestionControlExperiment(override val cpu: Int,
                                             override val memory: Int,
                                             override val hotspotLoad: Int) extends HotspotTpccExperiment {
  addAttribute("congestion")

  override def plotSymbol = "Caracal"

  override def cmdArguments() =
    super.cmdArguments() ++ Array("-XVHandleBatchAppend", "-XCongestionControl")
}

class HotspotTpccGranolaExperiment(override val cpu: Int,
                                   override val memory: Int,
                                   override val hotspotLoad: Int) extends HotspotTpccExperiment {
  addAttribute("granola")

  override def plotSymbol = "Granola"

  override def cmdArguments() =
    super.cmdArguments() ++ Array("-XEpochQueueLength20M", "-XEnableGranola")
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

class MultiNodeTpccExperiment(override val nodes: Int) extends BaseTpccExperiment(nodes) {
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
    0 until 3 foreach { _ =>
      for (cpu <- Seq(8, 16, 24, 32)) {
        for (contentionLevel <- Seq(0, 4)) {
          for (skewFactor <- Seq(0, 90)) {
            implicit val config = new YcsbExperimentConfig(cpu, cpu, skewFactor, contentionLevel)

            all.append(new YcsbLockingExperiment())
            all.append(new YcsbGranolaExperiment())
            all.append(new YcsbCaracalPieceExperiment())
            all.append(new YcsbCaracalSerialExperiment())
          }
        }
        for (skewFactor <- Seq(0, 90)) {
          implicit val config = new YcsbExperimentConfig(cpu, cpu, skewFactor, 7, true)

          // all.append(new YcsbCaracalPieceExperiment())
          all.append(new YcsbCaracalSerialExperiment())
          all.append(new YcsbGranolaExperiment())
        }
      }
    }
    run(all)
  }

  def runHotspotTpcc() = {
    val all = ArrayBuffer[Experiment]()
    0 until 3 foreach { _ =>
      for (cpu <- Seq(8, 16, 24, 32)) {
        for (load <- Seq(0, 200, 300, 400, 500)) {
          val mem = cpu * 2
          all.append(new HotspotTpccCongestionControlExperiment(cpu, mem, load))
          all.append(new HotspotTpccGranolaExperiment(cpu, mem, load))
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
      for (skewFactor <- Seq(0, 90)) {
        for (contentionLevel <- Seq(0, 4)) {
          implicit val config = new YcsbExperimentConfig(0, 0, skewFactor, contentionLevel)
          a.value ++= new YcsbLockingExperiment().loadResults().value
          a.value ++= new YcsbCaracalPieceExperiment().loadResults().value
          a.value ++= new YcsbCaracalSerialExperiment().loadResults().value
          a.value ++= new YcsbGranolaExperiment().loadResults().value          
        }
        implicit val config = new YcsbExperimentConfig(0, 0, skewFactor, 7)
        a.value ++= new YcsbCaracalSerialExperiment().loadResults().value
        a.value ++= new YcsbGranolaExperiment().loadResults().value
      }
      a
    }
  }

  def plotHotspotTpcc() = {
    plotTo("static/hotspot-tpcc.json") { a =>
      for (load <- Seq(0, 500)) {
        a.value ++= new HotspotTpccCongestionControlExperiment(0, 0, load).loadResults().value
        a.value ++= new HotspotTpccGranolaExperiment(0, 0, load).loadResults().value
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
  } else if (args(0) == "runHotspotTpcc") {
    runHotspotTpcc()
  } else if (args(0) == "plotHotspotTpcc") {
    plotHotspotTpcc()
  } else if (args(0) == "runMultiTpcc") {
    Experiment.ControllerHttp = "142.150.234.2:8666"
    Experiment.ControllerHost = "142.150.234.2:3148"
    runMultiTpcc()
  } else {
    println("mismatch")
  }

  sys.exit(0)
}
