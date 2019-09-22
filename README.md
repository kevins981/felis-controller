Run
---

We have switched from sbt to Mill. Mill is much faster than
sbt. Combining with the OpenJ9 VM installed on our cluster.

Setting Up OpenJ9 (Optional)
============================

This is optional, you can use Hotspot JDK as well (but make sure you
are using JDK11), but OpenJ9 is much faster due to its ability to AOT
Java code.

On our cluster, OpenJ9 (JDK11) is already installed at
`/pkg/java/j9`. You need to:

	export PATH=/pkg/java/j9/bin:$PATH
	export JAVA_HOME=/pkg/java/j9

Compile and Run
===============

Mill starts fast, you don't need to setup a daemon like sbt. Simply

	mill FelisController.run config.json

to run the controller. To build a jar, I recommend use

	mill FelisController.assembly

This will generate a standalone jar
`out/FelisController/assembly/dest/out.jar`. You can directly run with
`java -jar`.
