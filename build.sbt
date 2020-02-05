ThisBuild / scalaVersion := "2.13.1"

lazy val synchroiteration = (project in file("."))
	.settings(
		name := "SynchroIteration",
		libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
		libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.13.1"
	)
