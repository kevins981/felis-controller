Run
---

We have switched from sbt to Mill. Mill is much faster than sbt,
especially when combining with the OpenJ9 VM installed on our cluster.

Setting Up OpenJ9 (Optional)
============================

This is optional, you can use the Hotspot JDK as well (but make sure
you are using JDK11), but OpenJ9 is much faster due to its ability to
AOT Java code.

On our cluster, OpenJ9 (JDK11) is already installed at
`/pkg/java/j9`. You need to:

	export PATH=/pkg/java/j9/bin:$PATH
	export JAVA_HOME=/pkg/java/j9

Compile and Run
===============

To build a jar, use:

	mill FelisController.assembly

This will generate a standalone jar
`out/FelisController/assembly/dest/out.jar`. Usually, you can run that
jar directly, but if you are sharing the machine with someone else,
you need to avoid the cache dir conflict. For example:

	java -Dvertx.cacheDirBase=/tmp/$USER/ -jar out/FelisController/assembly/dest/out.jar


Running the Experiment Script
=============================

First, build a jar:

	mill FelisExperiments.assembly
	
Now you can run:

	java -jar out/FelisExperiments/assembly/dest/out.jar runXXX

For instance, `runYcsb` or `runHotspotTpcc`. See the code for further
details.

If you are sharing the machine with someone else, you can tell the
script to use your own port for the controller.

	java -Dcontroller.host=127.0.0.1:3148 -Dcontroller.http=127.0.0.1:8666 -jar out/FelisExperiments/assembly/dest/out.jar runXXX

