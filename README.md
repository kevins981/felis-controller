Run
---

We have switched from sbt to Mill. Mill is much faster than
sbt. Combining with the OpenJ9 VM installed on our cluster.

Setting Up OpenJ9 (Optional)
============================

This is optional, you can use Hotspot JDK as well, but OpenJ9 is much
faster due to its ability to AOT Java code.

On our cluster, OpenJ9 (JDK12) is already installed at
`/pkg/java/j9`. You need to:

	export PATH=/pkg/java/j9/bin:$PATH
	export JAVA_HOME=/pkg/java/j9

Compile and Run
===============

Mill supports interactive development. Simply

	mill FelisController.startServer

to start our server. This will automatically shutdown previous running
server too. If you need to stop the server:

	mill FelisController.stopServer

Our server is running inside the mill daemon. You may kill the mill
daemon to restart the whole build and run process.
