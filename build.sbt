ThisBuild / scalaVersion := "2.13.1"

lazy val synchroiteration = (project in file("."))
	.settings(
		name := "SynchroIteration",
		// libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2", // Is this needed for SynchroGMQL?
		// libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.13.1",  // Is this needed for SynchroGMQL?
		libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.6.2",  // imports os-lib convenience library
		mainClass in (Compile, run) := Some("synchrony.gmql_test.ParallelTests"),  // Select the main class of the test your are doing. Need fork for memory constraints
		fork in run := true,  // Needed for memory constraints
		// scalacOptions += "-optimize",
		// javaOptions ++= Seq("-Xms2G","-Xmx2G") // max JVM memory: 2048 MB
		// javaOptions ++= Seq("-Xms128M","-Xmx128M")  // max JVM memory: 128MB
	)
