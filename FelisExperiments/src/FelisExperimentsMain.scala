package edu.toronto.felis

import scala.collection.mutable.ArrayBuffer

class YcsbExperiment extends Experiment {
  override def boot(): Unit = {
    val args = Array(Experiment.Binary, "-c", Experiment.ControllerHost, "-n", "host1","-w", "ycsb") ++ cmdArguments()
    println(s"Making outdir ${outputDir()}")
    os.makeDir.all(os.Path(outputDir()))

    println(s"Booting with running ${args.mkString(" ")}")
    processes += os.proc(args).spawn(cwd = Experiment.WorkingDir, stderr = os.Inherit, stdout = os.Inherit)
  }
  override def loadAllResults(): Unit = ???
}

class YcsbPartitionExperiment(override val cpu: Int,
                              override val memory: Int,
                              override val skewFactor: Int,
                              override val contented: Boolean) extends YcsbExperiment with Contented with Skewed {
  addAttribute("partition")

  override def cmdArguments(): Array[String] =
    super.cmdArguments() ++ Array("-XYcsbEnablePartition", "-XEpochQueueLength200M", "-XVHandleLockElision")
}

object ExperimentsMain extends App {
  val all = ArrayBuffer[Experiment]()
  for (cpu <- Seq(8, 16, 24, 32)) {
    for (contented <- Seq(true, false)) {
      val mem = cpu
      all.append(new YcsbPartitionExperiment(cpu, mem, 0, contented))
    }
  }

  for (e <- all) {
    println(s"Running ${e.attributes.mkString(" + ")}")
    e.run()
  }
}
