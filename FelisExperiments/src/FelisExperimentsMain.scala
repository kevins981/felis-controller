package edu.toronto.felis

import scala.collection.mutable.{ArrayBuffer, HashMap}
import os.SubProcess
import scala.collection.mutable

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

abstract class BaseYcsbExperiment extends Experiment with YcsbContended with YcsbSkewed with YcsbDependency {
  override def boot(): Unit = {
    val args = Array(os.Path.expandUser(Experiment.Binary).toString,
      "-c", Experiment.ControllerHost,
      "-n", "host1",
      "-w", "ycsb") ++ cmdArguments()

    println(s"Making outdir ${outputDir()}")
    os.makeDir.all(os.Path.expandUser(outputDir()))

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
      (if (skewFactor > 0) Array("-XYcsbSkewFactor%03d".format(skewFactor)) else Array[String]()) ++
      (if (dependency) Array("-XYcsbDependency") else Array[String]())
  }
}

abstract class BaseYcsbFoedusExperiment(implicit val config: YcsbExperimentConfig) extends Experiment with YcsbContended with YcsbSkewed {
  addAttribute("dep")

  override def cpu = config.cpu
  override def memory = config.memory
  override def skewFactor = config.skewFactor
  override def contentionLevel = config.contentionLevel
  def hotThreshold = 10

  override def boot(): Unit = {}

  override def run(): Unit = {
    val args = Array(os.Path.expandUser("~/workspace/foedus/Release/experiments-core/src/foedus/ycsb/ycsb_hash").toString,
    "-thread_per_node=8", s"-numa_nodes=${cpu / 8}", "-snapshot_pool_size=1", "-reducer_buffer_size=1",
      "-loggers_per_node=2", "-volatile_pool_size=16",
      "-log_buffer_mb=512", "-duration_micro=10000000",
      "-workload=F", "-max_scan_length=1000", "-read_all_fields=1", "-write_all_fields=0",
      "-initial_table_size=10000000", "-random_inserts=0", "-ordered_inserts=0", "-sort_load_keys=0", "-fork_workers=true",
      "-verify_loaded_data=0", "-rmw_additional_reads=0", "-null_log_device=true", s"-hot_threshold=${hotThreshold}", "-sort_keys=0",
      "-extended_rw_lock=1", "-enable_retrospective_lock_list=0", "-extra_table_size=0",
      "-extra_table_rmws=0", "-extra_table_reads=0", "-distinct_keys=0", s"-caracal_output=${outputDir()}") ++ cmdArguments()

    println(s"Making outdir ${outputDir()}")
    os.makeDir.all(os.Path.expandUser(outputDir()))

    println(s"Running Foedus(Threshold = ${hotThreshold}) ${args.mkString(" ")}")
    spawnProcess(args)

    waitToFinish()
  }

  override def cmdArguments(): Array[String] = {
    (if (contentionLevel == 0) Array("-reps_per_tx=2", "-rmw_additional_reads=8") else Array("-reps_per_tx=10", "-rmw_additional_reads=0", "-caracal_contention=7")) ++
    Array("-zipfian_theta=0.%02d".format(skewFactor))
  }

  override def clean(): Unit = {
    os.remove.all(os.Path("/dev/shm/foedus_ycsb"))
  }
}

class YcsbFoedusExperiment(implicit override val config: YcsbExperimentConfig) extends BaseYcsbFoedusExperiment {
  addAttribute("foedus")
  override def plotSymbol = "Foedus(MOCC)"
}

class YcsbFoedusOCCExperiment(implicit override val config: YcsbExperimentConfig) extends BaseYcsbFoedusExperiment {
  addAttribute("foedus-occ")
  override def hotThreshold: Int = 255
  override def plotSymbol = "Foedus(OCC)"
}

class YcsbFoedus2PLExperiment(implicit override val config: YcsbExperimentConfig) extends BaseYcsbFoedusExperiment {
  addAttribute("foedus-2pl")
  override def hotThreshold: Int = 0
  override def plotSymbol = "Foedus(2PL)"
}

abstract class BaseYcsbSTOExperiment(implicit val config: YcsbExperimentConfig) extends Experiment with YcsbContended with YcsbSkewed {
  addAttribute("dep")
  
  override def cpu = config.cpu
  override def memory = config.memory
  override def skewFactor = config.skewFactor
  override def contentionLevel = config.contentionLevel
  def cc = "default"

  override def boot(): Unit = {}

  override def run(): Unit = {
    val args = Array(os.Path.expandUser("~/workspace/sto/ycsb_bench").toString,
      s"-i${cc}", s"-t${cpu}", "-l10", "-g", s"-z${outputDir()}") ++ cmdArguments()

    println(s"Making outdir ${outputDir()}")
    os.makeDir.all(os.Path.expandUser(outputDir()))

    println(s"Running SiloSTO2(${cc}) ${args.mkString(" ")}")

    spawnProcess(args)

    waitToFinish()
  }

  override def cmdArguments(): Array[String] = {
    (if (contentionLevel == 0) Array("-mB") else Array("-mA", s"-s${contentionLevel}")) ++
    Array("-e%02d".format(skewFactor))
  }
}

class YcsbErmiaExperiment(implicit val config: YcsbExperimentConfig) extends Experiment with YcsbContended with YcsbSkewed {
  addAttribute("dep")
  addAttribute("ermia")

  override def cpu = config.cpu
  override def memory = config.memory
  override def skewFactor = config.skewFactor
  override def contentionLevel = config.contentionLevel

  override def boot(): Unit = {}

  override def run(): Unit = {
    val args = Array(os.Path.expandUser("~/workspace/ermia-upstream/build/ermia_SI_SSN").toString,
      "-verbose", s"-caracal_outputdir=${outputDir()}", "-benchmark", "ycsb", "-threads", cpu.toString, "-scale_factor", cpu.toString,
      "-seconds", "10", "-node_memory_gb", (8 * memory / cpu).toString, "-log_data_dir", "/dev/shm/mike/ermia-log",  "-null_log_device",
      "-log_buffer_mb=128", "-log_segment_mb=16384", "-retry_aborted_transactions", "-backoff_aborted_transactions", "-enable_gc", "-benchmark_options") ++ cmdArguments()

    println(s"Making outdir ${outputDir()}")
    os.makeDir.all(os.Path.expandUser(outputDir()))

    println(s"Running Ermia SSN ${args.mkString(" ")}")

    spawnProcess(args)

    waitToFinish()
  }

  override def cmdArguments(): Array[String] = {
    val bench_opt = 
      (if (contentionLevel == 0) "--reps-per-tx 2 --rmw-additional-reads 8" else s"--reps-per-tx 10 --caracal-contention ${contentionLevel}") + (if (skewFactor > 0) " --zipfian --zipfian-theta %1.2f".format(0.01 * skewFactor) else "")

    Array(bench_opt)
  }

  override def plotSymbol = "Ermia"
}

class YcsbMSTOExperiment(implicit override val config: YcsbExperimentConfig) extends BaseYcsbSTOExperiment {
  addAttribute("msto")

  override def cc = "mvcc"
  override def plotSymbol = "MSTO"
}

class YcsbOSTOExperiment(implicit override val config: YcsbExperimentConfig) extends BaseYcsbSTOExperiment {
  addAttribute("osto")

  override def cc: String = "default"
  override def plotSymbol: String = "OSTO"
}

class YcsbTSTOExperiment(implicit override val config: YcsbExperimentConfig) extends BaseYcsbSTOExperiment {
  addAttribute("tsto")

  override def cc: String = "tictoc"
  override def plotSymbol: String = "TSTO"
}

// Baselines
class YcsbGranolaExperiment(implicit val config: YcsbExperimentConfig) extends BaseYcsbExperiment {
  addAttribute("granola")

  override def plotSymbol = "Granola"

  override def cmdArguments() =
    super.cmdArguments() ++ Array("-XYcsbEnablePartition", "-XEpochQueueLength100M", "-XEnableGranola")
}

class YcsbCaracalSerialExperiment(implicit val config: YcsbExperimentConfig, implicit var coreScalingThreshold: Int = -1) extends BaseYcsbExperiment {
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

class CaracalTuningConfig(val splittingThreshold: Int = -1, val optLevel: Int = 2) {}

trait CaracalTuningTrait extends Experiment {
  def addTuningAttributes(config: CaracalTuningConfig) = {
    if (config.optLevel == 2) {
      if (config.splittingThreshold != -1) {
        addAttribute(s"t${config.splittingThreshold}")
      }
    } else {
      addAttribute(s"O${config.optLevel}")
    }
  }

  def extraCmdArguments(tuningConfig: CaracalTuningConfig, defaultSplittingThresold: Int) = {
    if (tuningConfig.optLevel >= 2) { // Default
      // Default to 5
      val thresholdInVersions =
        if (tuningConfig.splittingThreshold == -1) defaultSplittingThresold
        else tuningConfig.splittingThreshold

      val extraArgs = ArrayBuffer[String](
        "-XVHandleBatchAppend",
        s"-XOnDemandSplitting${thresholdInVersions}")

      if (tuningConfig.optLevel == 9) {
        extraArgs += "-XBinpackSplitting"
      }

      extraArgs.toArray
    } else if (tuningConfig.optLevel == 1) {
      Array("-XVHandleBatchAppend")
    } else if (tuningConfig.optLevel == 0) {
      Array[String]()
    } else {
      println(s"Unknown OptLevel ${tuningConfig.optLevel}!!!")
      sys.exit(-1)
    }
  }
}

class YcsbCaracalPieceExperiment(
  implicit val config: YcsbExperimentConfig,
  implicit val tuningConfig: CaracalTuningConfig = new CaracalTuningConfig())
    extends BaseYcsbExperiment with CaracalTuningTrait {

  addAttribute("caracal-pieces")
  addTuningAttributes(tuningConfig)

  override def plotSymbol = "Caracal"

  override def cmdArguments() = {
    var default: Long = if (contentionLevel == 0) 4096 else 512
    if (epochSize > 0) default = Math.max(2, default * config.epochSize / 100000)
    super.cmdArguments() ++ extraCmdArguments(tuningConfig, default.toInt)
  }
}

class TpccExperimentConfig(
  val cpu: Int,
  val memory: Int,
  val nodes: Int = 1,
  val epochSize: Int = -1,
  val singleWarehouse: Boolean = false)
{}

abstract class BaseTpccExperiment(implicit val config: TpccExperimentConfig) extends Experiment {
  if (config.singleWarehouse) {
    addAttribute("singlewarehouse-tpcc")
  } else if (config.nodes == 1) {
    addAttribute("singlenode-tpcc")
  } else {
    addAttribute("distributed-tpcc")
  }
  
  def nodes = config.nodes
  def warehouses = if (config.singleWarehouse) 1 else config.cpu * config.nodes

  override def cpu = config.cpu
  override def memory = config.memory
  override def epochSize = config.epochSize
  
  override def boot() = {
    println(s"Making outdir ${outputDir()}")
    os.makeDir.all(os.Path.expandUser(outputDir()))
    println(s"Total number of warehouses ${warehouses}")

    for (i <- 1 to nodes) {
      val nodeName = s"host${i}"
      val args = Array(Experiment.Binary, "-c", Experiment.ControllerHost, "-n", nodeName, "-w", "tpcc",
        s"-XMaxNodeLimit${nodes}", s"-XTpccWarehouses${warehouses}") ++ cmdArguments()

      launchProcess(nodeName, args)
    }
  }

  def launchProcess(nodeName: String, args: Seq[String]) = {
    println(s"Launching process with ${args.mkString(" ")}")
  }
}

abstract class SingleNodeTpccExperiment(implicit override val config: TpccExperimentConfig) extends BaseTpccExperiment {
  override def launchProcess(nodeName: String, args: Seq[String]): Unit = {
    super.launchProcess(nodeName, args)
    // Ignoring the nodeName because this is a single node experiment
    spawnProcess(Seq(os.Path.expandUser(Experiment.Binary).toString) ++ args.drop(1))
  }
}

class TpccCaracalExperiment(
  implicit override val config: TpccExperimentConfig,
  implicit val tuningConfig: CaracalTuningConfig = new CaracalTuningConfig())
    extends SingleNodeTpccExperiment with CaracalTuningTrait {

  addAttribute("caracal")
  addTuningAttributes(tuningConfig)

  override def plotSymbol = "Caracal"
  override def cmdArguments() = {
    var default: Long = (if (warehouses == 1) 4 else 1000000)
    if (epochSize > 0) default = Math.max(2, default * epochSize / 100000)
    super.cmdArguments() ++ extraCmdArguments(tuningConfig, default.toInt)
  }
}

class TpccGranolaExperiment(implicit override val config: TpccExperimentConfig) extends SingleNodeTpccExperiment {
  addAttribute("granola")

  override def plotSymbol = "Granola"
  override def cmdArguments() =
    super.cmdArguments() ++ Array("-XEnableGranola")
}

abstract class BaseTpccSTOExperiment(implicit override val config: TpccExperimentConfig) extends BaseTpccExperiment {
  def cc = "default"
  override def boot(): Unit = {}
  override def run(): Unit = {
    os.makeDir.all(os.Path.expandUser(outputDir()))
    val args = Seq(os.Path.expandUser("~/workspace/sto/tpcc_bench").toString(),
      s"-i${cc}", s"-t${cpu}", s"-w${warehouses}", "-l20", "-g", "-r1000", s"-z${outputDir()}")

    spawnProcess(args)

    waitToFinish()
  }
}

class TpccMSTOExperiment(implicit override val config: TpccExperimentConfig) extends BaseTpccSTOExperiment {
  addAttribute("msto")
  override def cc = "mvcc"
  override def plotSymbol = "MSTO"
}

class TpccOSTOExperiment(implicit override val config: TpccExperimentConfig) extends BaseTpccSTOExperiment {
  addAttribute("osto")
  override def cc = "default"
  override def plotSymbol = "OSTO"
}

class TpccTSTOExperiment(implicit override val config: TpccExperimentConfig) extends BaseTpccSTOExperiment {
  addAttribute("tsto")
  override def cc = "tictoc"
  override def plotSymbol = "TSTO"
}

abstract class BaseTpccFoedusExperiment(implicit override val config: TpccExperimentConfig) extends BaseTpccExperiment {
  def hcc = 0
  override def boot(): Unit = {}
  override def run(): Unit = {
    os.makeDir.all(os.Path.expandUser(outputDir()))
    val args = Seq(os.Path.expandUser("~/workspace/foedus/Release/experiments-core/src/foedus/tpcc/tpcc").toString,
      s"-warehouses=${warehouses}", "-fork_workers=true", "-nvm_folder=/dev/shm", "-high_priority=false", "-null_log_device=true", "-loggers_per_node=2",
      "-thread_per_node=8", s"-numa_nodes=${cpu / 8}", "-log_buffer_mb=1024", "-neworder_remote_percent=1", "-payment_remote_percent=15", "-volatile_pool_size=16",
      "-snapshot_pool_size=1", "-reducer_buffer_size=2", "-duration_micro=10000000", s"-hcc_policy=${hcc}", s"-caracal_output=${outputDir()}",
    )
    spawnProcess(args)

    waitToFinish()
  }
  override def clean(): Unit = {
    os.remove.all(os.Path("/dev/shm/foedus_tpcc"))
  }
}

class TpccErmiaExperiment(implicit override val config: TpccExperimentConfig) extends BaseTpccExperiment {
  addAttribute("ermia")

  override def boot(): Unit = {}
  override def run(): Unit = {
    val args = Array(os.Path.expandUser("~/workspace/ermia-upstream/build/ermia_SI_SSN").toString,
      "-verbose", s"-caracal_outputdir=${outputDir()}", "-benchmark", "tpcc", "-threads", cpu.toString, "-scale_factor", warehouses.toString,
      "-seconds", "10", "-node_memory_gb", (8 * memory / cpu).toString, "-log_data_dir", "/dev/shm/mike/ermia-log",  "-null_log_device",
      "-log_buffer_mb=128", "-log_segment_mb=16384", "-retry_aborted_transactions", "-backoff_aborted_transactions", "-enable_gc", "-benchmark_options", "--new-order-fast-id-gen --order-status-scan-hack")

    println(s"Making outdir ${outputDir()}")
    os.makeDir.all(os.Path.expandUser(outputDir()))

    println(s"Running Ermia SSN ${args.mkString(" ")}")

    spawnProcess(args)
    waitToFinish()
  }

  override def plotSymbol = "Ermia"
}

class TpccFoedusMOCCExperiment(implicit override val config: TpccExperimentConfig) extends BaseTpccFoedusExperiment {
  addAttribute("foedus")
  override def hcc: Int = 0
  override def plotSymbol = "Foedus(MOCC)"
}

class TpccFoedusOCCExperiment(implicit override val config: TpccExperimentConfig) extends BaseTpccFoedusExperiment {
  addAttribute("foedus-occ")
  override def hcc: Int = 1
  override def plotSymbol = "Foedus(OCC)"
}

class TpccFoedus2PLExperiment(implicit override val config: TpccExperimentConfig) extends BaseTpccFoedusExperiment {
  addAttribute("foedus-2pl")
  override def hcc: Int = 2
  override def plotSymbol = "Foedus(2PL)"
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

  ExperimentSuite("Ycsb", "Ycsb on Foedus/Silo/Caracal/Caracal/Ermia") { runs: ArrayBuffer[Experiment] =>
    def setupExperiments(cfg: YcsbExperimentConfig) = {
      implicit val config = cfg

      // runs.append(new YcsbErmiaExperiment())
      // runs.append(new YcsbFoedus2PLExperiment())
      // runs.append(new YcsbCaracalPieceExperiment())
      // runs.append(new YcsbMSTOExperiment())
      // runs.append(new YcsbOSTOExperiment())
      runs.append(new YcsbTSTOExperiment())

      // Deprecated:
      // runs.append(new YcsbCaracalSerialExperiment())
      // runs.append(new YcsbFoedusExperiment())
      // runs.append(new YcsbFoedusOCCExperiment())
      // runs.append(new YcsbGranolaExperiment())
    }

    for (cpu <- Seq(8, 16, 24, 32)) {
      for (contend <- Seq(false, true)) {
        for (skewFactor <- Seq(0, 90)) {
          val mem = Math.max(cpu * 2, 64)
          for (cfg <- Seq(new YcsbExperimentConfig(cpu, mem, skewFactor, if (contend) 7 else 0))) {
            setupExperiments(cfg)
          }
        }
      }
      for (skewFactor <- Seq(30, 60, 120)) {
        val mem = Math.max(cpu * 2, 32)
        for (cfg <- Seq(new YcsbExperimentConfig(cpu, mem, skewFactor, 0))) {
          setupExperiments(cfg)
        }
      }
    }
  }

  def tuningYcsbExperiements(cpu: Int = 32)(implicit tuningConfig: CaracalTuningConfig) = {
    val runs = ArrayBuffer[BaseYcsbExperiment]()

    for (cfg <- Seq(
      new YcsbExperimentConfig(cpu, 32, 90, 0, false),
      new YcsbExperimentConfig(cpu, 32, 0, 7, false),
      new YcsbExperimentConfig(cpu, 32, 90, 7, false))) {

      implicit val config = cfg
      runs.append(new YcsbCaracalPieceExperiment())
    }

    runs
  }

  def tuningTpccExperiments(cpu: Int = 32)(implicit tuningConfig: CaracalTuningConfig) = {
    val runs = ArrayBuffer[BaseTpccExperiment]()

    {
      implicit val config = new TpccExperimentConfig(cpu, 16, 1, -1, true)
      runs.append(new TpccCaracalExperiment())
    }

    runs
  }

  val ycsbTuningRange = (1 to 20).map(Math.pow(2, _).toInt)
  val tpccTuningRange = (1 to 9).map(Math.pow(2, _).toInt)

  ExperimentSuite("Tuning", "Tuning thresholds") {
    runs: ArrayBuffer[Experiment] =>

    ycsbTuningRange.foreach {
      splittingThreshold =>
      implicit val tuningConfig = new CaracalTuningConfig(splittingThreshold)
      runs ++= tuningYcsbExperiements()
    }

    tpccTuningRange.foreach {
      splittingThreshold =>
      implicit val tuningConfig = new CaracalTuningConfig(splittingThreshold)
      runs ++= tuningTpccExperiments()
    }

  }
  
  ExperimentSuite("TestOpt", "Test how our optimzations") {
    runs: ArrayBuffer[Experiment] =>

    for (optLevel <- Seq(0, 1, 9)) {
      implicit val tuningConfig = new CaracalTuningConfig(-1, optLevel)
      for (cpu <- Seq(8, 16, 24, 32)) {
        runs ++= tuningYcsbExperiements(cpu)
        runs ++= tuningTpccExperiments(cpu)
      }
    }
  }

  ExperimentSuite("TpccSingle", "Single Node TPC-C") {
    runs: ArrayBuffer[Experiment] =>

    for (cpu <- Seq(8, 16, 24, 32)) {
      for (singleWarehouse <- Seq(false, true)) {
        implicit val config = new TpccExperimentConfig(cpu, cpu * 2, 1, -1, singleWarehouse)
        // runs.append(new TpccCaracalExperiment())
        // runs.append(new TpccOSTOExperiment())
        // runs.append(new TpccMSTOExperiment())
        runs.append(new TpccTSTOExperiment())
        // runs.append(new TpccFoedus2PLExperiment())
        // runs.append(new TpccErmiaExperiment())

        // Deprecated:
        // runs.append(new TpccGranolaExperiment())
        // runs.append(new TpccFoedusMOCCExperiment())
        // runs.append(new TpccFoedusOCCExperiment())
      }
    }
  }

  ExperimentSuite("EpochSizeTuning", "Tpcc with different epoch sizes") {
    runs: ArrayBuffer[Experiment] =>

    
    for (epochSize <- 5000 to 100000 by 5000) {
      for (contention <- Seq(0, 7)) {
        for (skewFactor <- Seq(0, 90)) {
          implicit val config = new YcsbExperimentConfig(32, 64, skewFactor, contention, false, epochSize)
          runs.append(new YcsbCaracalPieceExperiment())
        }
      }
     
      for (singleWarehouse <- Seq(false, true)) {
        implicit val config = new TpccExperimentConfig(32, 64, 1, epochSize, singleWarehouse)
        runs.append(new TpccCaracalExperiment())
      }
    }
  }

  PlotSuite("Ycsb", "static/ycsb.json") { () =>
    val a = ujson.Arr()

    def loadResults(cfg: YcsbExperimentConfig) = {
      implicit val config = cfg
      a.value ++= new YcsbFoedus2PLExperiment().loadResults().value
      a.value ++= new YcsbCaracalPieceExperiment().loadResults().value
      a.value ++= new YcsbMSTOExperiment().loadResults().value
      a.value ++= new YcsbOSTOExperiment().loadResults().value
      a.value ++= new YcsbTSTOExperiment().loadResults().value
      a.value ++= new YcsbErmiaExperiment().loadResults().value

      // a.value ++= new YcsbGranolaExperiment().loadResults().value
      // a.value ++= new YcsbFoedusExperiment().loadResults().value
      // a.value ++= new YcsbFoedusOCCExperiment().loadResults().value
      // a.value ++= new YcsbCaracalSerialExperiment().loadResults().value
    }

    for (skewFactor <- Seq(0, 90)) {
      for (contend <- Seq(true, false)) {
        loadResults(new YcsbExperimentConfig(0, 0, skewFactor, if (contend) 7 else 0))
      }
    }
    for (skewFactor <- Seq(30, 60, 120)) {
      loadResults(new YcsbExperimentConfig(0, 0, skewFactor, 0))
    }

    a
  }

  def addThresholdIntoObject(arr: ArrayBuffer[ujson.Value], workload: String)(implicit tuningConfig: CaracalTuningConfig) = {
      arr.foreach {
        x =>

        x.obj.put("threshold", tuningConfig.splittingThreshold)
        x.obj.put("workload", workload);
      }
      arr
  }

  PlotSuite("TestOpt", "static/test-opt.json") { () =>
    val a = ujson.Arr()

    for (optLevel <- Seq(0, 1, 2, 9)) {
      implicit val tuningConfig = new CaracalTuningConfig(-1, optLevel)
      val experiments = tuningYcsbExperiements() ++ tuningTpccExperiments()
      a.value ++= experiments.flatMap {
        w =>
        val arr = w.loadResults().value
        arr.foreach {
          x =>
          x.obj.put("symbol",
            if (optLevel == 0) "No Optimization"
            else if (optLevel == 1) "Batch Append"
            else if (optLevel == 2) "Caracal"
            else if (optLevel == 9) "Bin Packing Placement"
            else "")
        }
        arr
      }
    }

    a
  }

  PlotSuite("YcsbTuning", "static/ycsb-tuning.json") { () =>
    val a = ujson.Arr()

    ycsbTuningRange.foreach {
      splittingThreshold =>
      implicit val tuningConfig = new CaracalTuningConfig(splittingThreshold)

      a.value ++= tuningYcsbExperiements().flatMap {
        exp =>
        val label = "YCSB" + (if (exp.contentionLevel > 0) "+Contention" else "") + (if (exp.skewFactor > 0) "+Skew" else "")

        addThresholdIntoObject(exp.loadResults().value, label)
      }
    }

    a
  }

  PlotSuite("TpccTuning", "static/tpcc-tuning.json") { () =>
    val a = ujson.Arr()

    tpccTuningRange.foreach {
      splittingThreshold =>
      implicit val tuningConfig = new CaracalTuningConfig(splittingThreshold)

      a.value ++= tuningTpccExperiments().flatMap {
        exp =>

        addThresholdIntoObject(exp.loadResults().value, "TPC-C" + (if (exp.warehouses == 1) " Single Warehouse" else ""))
      }
    }
    
    a
  }


/*
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
 */

  PlotSuite("EpochSizeTuning", "static/epoch-size.json") { () =>
    val a = ujson.Arr()

    for (epochSize <- 5000 to 100000 by 5000) {
      val epochArr = ArrayBuffer[ujson.Value]()

      for (contention <- Seq(0, 7)) {
        for (skewFactor <- Seq(0, 90)) {
          implicit val config = new YcsbExperimentConfig(32, 64, skewFactor, contention, false, epochSize)
          epochArr ++= (new YcsbCaracalPieceExperiment().loadResults().value).map {
            x =>

            x.obj.put("workload", "YCSB"
              + (if (contention > 0) "+Contention" else "")
              + (if (skewFactor > 0) "+Skew" else ""))

            x
          }
        }
      }

      for (singleWarehouse <- Seq(true, false)) {
        implicit val config = new TpccExperimentConfig(32, 64, 1, epochSize, singleWarehouse)
        epochArr ++= new TpccCaracalExperiment().loadResults().value.map {
          x =>

          x.obj.put("workload", "TPC-C"
            + (if (singleWarehouse) " Single Warehouse" else ""))

          x
        }
      }

      epochArr.foreach {
        x =>
        val thru = x.obj.get("throughput").get.num
        x.obj.put("latency", 1000 * epochSize / thru); // in ms
      }

      a.value ++= epochArr
    }

    a
  }

  PlotSuite("TpccSingle", "static/single-tpcc.json") { () =>
    val a = ujson.Arr()
    implicit val config = new TpccExperimentConfig(0, 0)

    for (cpu <- Seq(8, 16, 24, 32)) {
      for (singleWarehouse <- Seq(false, true)) {
        implicit val config = new TpccExperimentConfig(cpu, cpu * 2, 1, -1, singleWarehouse)
        a.value ++= new TpccCaracalExperiment().loadResults().value
        a.value ++= new TpccOSTOExperiment().loadResults().value
        a.value ++= new TpccTSTOExperiment().loadResults().value
        a.value ++= new TpccMSTOExperiment().loadResults().value
        a.value ++= new TpccFoedus2PLExperiment().loadResults().value
        a.value ++= new TpccErmiaExperiment().loadResults().value
      }
    }

    /*
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
     */
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
