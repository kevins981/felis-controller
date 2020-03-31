package edu.toronto.felis

import scala.collection.mutable.{ArrayBuffer, HashMap}

class YcsbExperimentConfig(
  val cpu: Int,
  val memory: Int,
  val skewFactor: Int,
  val contentionLevel: Int,
  val dependency: Boolean = false,
  val epochSize: Int = -1)
{}

// Configurations in the experiments
trait YcsbContended extends Experiment {
  def contentionLevel = 0
  addAttribute(s"cont${contentionLevel}")
}

trait YcsbSkewed extends Experiment {
  def skewFactor = 0
  addAttribute(if (skewFactor == 0) "noskew" else "skew%02d".format(skewFactor))
}

trait YcsbDependency extends Experiment {
  def dependency = false
  addAttribute(if (dependency) "dep" else "nodep")
}

abstract class YcsbExperiment extends Experiment with YcsbContended with YcsbSkewed with YcsbDependency {
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
  override def dependency = config.dependency
  override def epochSize = config.epochSize

  override def cmdArguments(): Array[String] = {
    super.cmdArguments() ++
      (if (contentionLevel == 0) Array("-XYcsbReadOnly8") else Array(s"-XYcsbContentionKey${contentionLevel}")) ++
      (if (skewFactor > 0) Array("-XYcsbSkewFactor%02d".format(skewFactor)) else Array[String]()) ++
      (if (dependency) Array("-XYcsbDependency") else Array[String]())
  }
}

class YcsbFoedusExperiment(implicit val config: YcsbExperimentConfig) extends Experiment with YcsbContended with YcsbSkewed {
  addAttribute("dep")
  addAttribute("foedus")

  override def cpu = config.cpu
  override def memory = config.memory
  override def skewFactor = config.skewFactor
  override def contentionLevel = config.contentionLevel

  override def boot(): Unit = {}

  override def run(): Unit = {
    val args = Array(os.Path.expandUser("~/workspace/foedus/Release/experiments-core/src/foedus/ycsb/ycsb_masstree").toString,
    "-thread_per_node=8", s"-numa_nodes=${cpu / 8}", "-snapshot_pool_size=1", "-reducer_buffer_size=1",
      "-loggers_per_node=2", "-volatile_pool_size=16",
      "-log_buffer_mb=512", "-duration_micro=10000000",
      "-workload=F", "-max_scan_length=1000", "-read_all_fields=1", "-write_all_fields=0",
      "-initial_table_size=16777216", "-random_inserts=0", "-ordered_inserts=0", "-sort_load_keys=0", "-fork_workers=true",
      "-verify_loaded_data=0", "-rmw_additional_reads=0", "-null_log_device=true", "-hot_threshold=32", "-sort_keys=0",
      "-extended_rw_lock=1", "-enable_retrospective_lock_list=0", "-extra_table_size=0",
      "-extra_table_rmws=0", "-extra_table_reads=0", "-distinct_keys=0", s"-caracal_output=${outputDir()}") ++ cmdArguments()

    println(s"Making outdir ${outputDir()}")
    os.makeDir.all(os.Path.expandUser(outputDir()))

    println(s"Running Foedus ${args.mkString(" ")}")
    spawnProcess(args)

    waitToFinish()
  }

  override def cmdArguments(): Array[String] = {
    if (contentionLevel == 0) Array("-reps_per_tx=2", "-rmw_additional_reads=8") else Array("-reps_per_tx=10", "-rmw_additional_reads=0", "-caracal_contention=7") ++
    Array("-zipfian_theta=0.%02d".format(skewFactor))
  }

  override def plotSymbol = "Foedus"
}

// Baselines
class YcsbGranolaExperiment(implicit val config: YcsbExperimentConfig) extends YcsbExperiment {
  addAttribute("granola")

  override def plotSymbol = "Granola"

  override def cmdArguments() =
    super.cmdArguments() ++ Array("-XYcsbEnablePartition", "-XEpochQueueLength100M", "-XEnableGranola")
}

class YcsbLockingExperiment(implicit val config: YcsbExperimentConfig) extends YcsbExperiment {
  addAttribute("locking")

  override def plotSymbol = "Baseline"
}

class YcsbCaracalSerialExperiment(implicit val config: YcsbExperimentConfig, implicit var coreScalingThreshold: Int = -1) extends YcsbExperiment {
  addAttribute("caracal-serial")
  if (coreScalingThreshold == -1) {
    coreScalingThreshold = 8
  } else {
    addAttribute(s"t${coreScalingThreshold}")
  }

  override def plotSymbol = "Serial Caracal"

  override def cmdArguments() =
    super.cmdArguments() ++ Array("-XVHandleBatchAppend", s"-XCoreScaling${coreScalingThreshold}")
}

class YcsbCaracalPieceExperiment(implicit val config: YcsbExperimentConfig, implicit var parallelThreshold: Int = -1) extends YcsbExperiment {
  addAttribute("caracal-pieces")
  if (parallelThreshold == -1) {
    parallelThreshold = 4096
  } else {
    addAttribute(s"t${parallelThreshold}")
  }

  override def plotSymbol = "Parallel Caracal"

  override def cmdArguments() =
    super.cmdArguments() ++ Array("-XVHandleBatchAppend", s"-XVHandleParallel${parallelThreshold}")
}

class TpccExperimentConfig(
  val cpu: Int,
  val memory: Int,
  val nodes: Int = 1,
  val epochSize: Int = -1)
{}

class BaseTpccExperiment(implicit val config: TpccExperimentConfig) extends Experiment {

  def nodes = config.nodes

  override def cpu = config.cpu
  override def memory = config.memory
  override def epochSize = config.epochSize

  override def boot() = {
    println(s"Making outdir ${outputDir()}")
    os.makeDir.all(os.Path.expandUser(outputDir()))
    val warehouses = nodes * cpu
    println(s"Total number of warehouses ${warehouses}")

    for (i <- 1 to nodes) {
      val nodeName = s"host${i}"
      val args = Array(Experiment.Binary, "-c", Experiment.ControllerHost, "-n", nodeName, "-w", "tpcc",
        "-XEpochQueueLength1m", s"-XMaxNodeLimit${nodes}", s"-XTpccWarehouses${warehouses}") ++ cmdArguments()

      launchProcess(nodeName, args)
    }
  }

  def launchProcess(nodeName: String, args: Seq[String]) = {
    println(s"Launching process with ${args.mkString(" ")}")
  }
}

class HotspotTpccExperiment(implicit override val config: TpccExperimentConfig, implicit val hotspotLoad: Int = 0) extends BaseTpccExperiment {
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

class HotspotTpccCaracalExperiment(
  implicit override val config: TpccExperimentConfig,
  implicit override val hotspotLoad: Int = 0) extends HotspotTpccExperiment {
  addAttribute("caracal")

  override def plotSymbol = "Caracal"

  override def cmdArguments() =
    super.cmdArguments() ++ Array("-XVHandleBatchAppend", "-XCoreScaling10")
}

class HotspotTpccGranolaExperiment(
  implicit override val config: TpccExperimentConfig,
  implicit override val hotspotLoad: Int = 0) extends HotspotTpccExperiment {
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
/*
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
 */

object ExperimentsMain extends App {

  ExperimentSuite("Ycsb", "Ycsb on Foedus/Granola/RBR(Locking)/Caracal") { runs: ArrayBuffer[Experiment] =>
    for (cpu <- Seq(8, 16, 24, 32)) {
      for (contend <- Seq(false, true)) {
        for (skewFactor <- Seq(0, 90)) {
          val mem = cpu * 2
          for (cfg <- Seq(new YcsbExperimentConfig(cpu, mem, skewFactor, if (contend) 7 else 0), new YcsbExperimentConfig(cpu, mem, skewFactor, if (contend) 7 else 0, true))) {
            implicit val config = cfg

            runs.append(new YcsbFoedusExperiment())
            runs.append(new YcsbLockingExperiment())
            runs.append(new YcsbGranolaExperiment())
            runs.append(new YcsbCaracalPieceExperiment())
            runs.append(new YcsbCaracalSerialExperiment())
          }
        }
      }
    }
  }

  ExperimentSuite("YcsbTuning", "Tuning thresholds") { runs: ArrayBuffer[Experiment] =>
    for (cfg <- Seq(new YcsbExperimentConfig(32, 32, 0, 7, true), new YcsbExperimentConfig(32, 32, 90, 0, true))) {
      implicit val config = cfg
      for (threshold <- (1 to 14).map(x => math.pow(2, x - 1).toInt) ++ (9 * 1024 until 16 * 1024 by 1024)) {
        implicit val parallelThreshold = threshold
        runs.append(new YcsbCaracalPieceExperiment())
      }

      for (threshold <- (1 to 12)) {
        implicit val coreScalingThreshold = threshold
        runs.append(new YcsbCaracalSerialExperiment())
      }
    }
  }

  ExperimentSuite("TpccSingle", "Tpcc with different hotspots (or no hotspot at all)") { runs: ArrayBuffer[Experiment] =>
    for (cpu <- Seq(8, 16, 24, 32)) {
      for (load <- Seq(0, 200, 300, 400)) {
        implicit val config = new TpccExperimentConfig(cpu, cpu * 2)
        implicit val hotspotLoad = load
        runs.append(new HotspotTpccCaracalExperiment())
        runs.append(new HotspotTpccGranolaExperiment())
      }
    }
  }

  ExperimentSuite("EpochSizeTuning", "Tpcc with different epoch sizes") { runs: ArrayBuffer[Experiment] =>
    for (epochSize <- 5000 to 100000 by 5000) {
      for (cfg <- Seq(new YcsbExperimentConfig(32, 32, 0, 0, true, epochSize), new YcsbExperimentConfig(32, 32, 90, 0, true, epochSize))) {
        implicit val config = cfg
        runs.append(new YcsbCaracalPieceExperiment())
      }
      implicit val config = new TpccExperimentConfig(32, 64, 1, epochSize)
      runs.append(new HotspotTpccCaracalExperiment())
    }
  }

  PlotSuite("Ycsb", "static/ycsb.json") { () =>
    val a = ujson.Arr()
    for (skewFactor <- Seq(0, 90)) {
      for (contend <- Seq(true, false)) {
        for (cfg <- Seq(new YcsbExperimentConfig(0, 0, skewFactor, if (contend) 7 else 0),
          new YcsbExperimentConfig(0, 0, skewFactor, if (contend) 7 else 0, true))) {
          implicit val config = cfg
          a.value ++= new YcsbFoedusExperiment().loadResults().value
          a.value ++= new YcsbLockingExperiment().loadResults().value
          a.value ++= new YcsbCaracalPieceExperiment().loadResults().value
          a.value ++= new YcsbCaracalSerialExperiment().loadResults().value
          a.value ++= new YcsbGranolaExperiment().loadResults().value
        }
      }
    }

    for (cfg <- Seq(new YcsbExperimentConfig(0, 0, 0, 7, true), new YcsbExperimentConfig(0, 0, 90, 0, true))) {
      implicit val config = cfg
      for (threshold <- (1 to 14).map(x => math.pow(2, x - 1).toInt) ++ (9 * 1024 until 16 * 1024 by 1024)) {
        implicit val parallelThreshold = threshold
        val value = new YcsbCaracalPieceExperiment().loadResults().value
        value.foreach(x => x.obj.put("threshold", threshold.toDouble / 10000))
        a.value ++= value
      }
      for (threshold <- (1 to 12)) {
        implicit val coreScalingThreshold = threshold
        val value = new YcsbCaracalSerialExperiment().loadResults().value
        value.foreach(x => x.obj.put("threshold", threshold))
        a.value ++= value
      }
    }

    for (epochSize <- 5000 until 100000 by 5000) {
      for (cfg <- Seq(new YcsbExperimentConfig(0, 0, 0, 0, true, epochSize), new YcsbExperimentConfig(0, 0, 90, 0, true, epochSize))) {
        implicit val config = cfg
        val value = new YcsbCaracalPieceExperiment().loadResults().value
        value.foreach(x => x.obj.put("latency", 1000.0 * epochSize / x.obj.get("throughput").get.num))
        a.value ++= value
      }
    }
    a
  }

  PlotSuite("TpccSingle", "static/hotspot-tpcc.json") { () =>
    val a = ujson.Arr()
    implicit val config = new TpccExperimentConfig(0, 0)
    for (load <- Seq(0, 200, 300, 400)) {
      implicit val hotspotLoad: Int = load
      a.value ++= new HotspotTpccCaracalExperiment().loadResults().value
      a.value ++= new HotspotTpccGranolaExperiment().loadResults().value
    }

    for (epochSize <- 5000 until 100000 by 5000) {
      implicit val config = new TpccExperimentConfig(32, 64, 1, epochSize)
      val value = new HotspotTpccCaracalExperiment().loadResults().value
      value.foreach(x => x.obj.put("latency", 1000.0 * epochSize / x.obj.get("throughput").get.num))
      a.value ++= value
    }

    a
  }


  if (args.length == 0) {
    ExperimentSuite.show()
    PlotSuite.show()
    sys.exit(-1)
  }

  if (args(0).startsWith("run")) {
    ExperimentSuite.invoke(args(0).substring(3))
  } else if (args(0).startsWith("plot")) {
    PlotSuite.invoke(args(0).substring(4))
  }

}
