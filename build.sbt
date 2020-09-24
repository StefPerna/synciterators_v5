ThisBuild / scalaVersion := "2.13.1"

lazy val synchroiteration = (project in file("."))
	.settings(
		name := "SynchroIteration",
		resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
		libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
		libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.13.1",
		libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.6.2",
		mainClass in (Compile, run) := Some("demo.GMQLSynchro_Demo2"),
		fork in run := true,
		// javaOptions ++= Seq("-Xms128M","-Xmx128M","-XX:+PrintGCDetails","-Xloggc:gclog.txt")
		// Info on GC options: https://dzone.com/articles/enabling-and-analysing-the-garbage-collection-log
		//javaOptions ++= Seq("-Xms2G","-Xmx2G","-Xlog:gc*","-Xlog:gc:gclog.txt")
	)
