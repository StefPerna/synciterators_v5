ThisBuild / scalaVersion := "2.13.1"

lazy val synchroiteration = (project in file("."))
	.settings(
		name := "SynchroIteration",
		libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
		libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.13.1",
		libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.6.2",
		mainClass in (Compile, run) := Some("demo.GMQLSynchro_Demo"),
		fork in run := true,
		// javaOptions ++= Seq("-Xms128M","-Xmx128M")
		javaOptions ++= Seq("-Xms2G","-Xmx2G")
	)
