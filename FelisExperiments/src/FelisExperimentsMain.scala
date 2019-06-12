package edu.toronto.felis

import scala.collection.mutable.ArrayBuffer

class YcsbExperiment extends Experiment {
  override def boot(): Unit = {
    val args = Array(Experiment.Binary, "-c", Experiment.ControllerHost, "-n", "host1","-w", "ycsb") ++ cmdArguments()
    println("Booting with running %s".format(args.mkString(" ")))
    processes += os.proc(args).spawn(cwd = Experiment.WorkingDir)
  }
  override def loadAllResults(): Unit = ???
}

class YcsbPartitionExperiment(override val cpu: Int,
                              override val skewFactor: Int,
                              override val contented: Boolean) extends YcsbExperiment with Contented with Skewed {
  override def cmdArguments(): Array[String] =
    super.cmdArguments() ++ Array("-XYcsbEnablePartition", "-XEpochQueueLength200M", "-XVHandleLockElision")
}

object ExperimentsMain extends App {
  val all = ArrayBuffer[Experiment]()
  for (cpu <- Seq(8, 16, 24, 32)) {
    for (contented <- Seq(true, false)) {
      all.append(new YcsbPartitionExperiment(cpu, 0, contented))
    }
  }

  for (e <- all) {
    println(s"Running ${e.attributes.mkString(" + ")}")
    e.run()
  }
}
