version := "0.1"
val compilerVersion = "2.11.12"


val buildSettings = Seq(
  name := "spark_with_scala",
  scalaVersion := compilerVersion,
  description := "Simple project to follow a course",
  resolvers := Resolvers.defaultResoulvers,
  libraryDependencies ++= Dependencies.rootDependencies,
  scalaBinaryVersion in ThisBuild := "2.11",
  fork in Test := true,
  javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
  run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run)).evaluated,
  parallelExecution in Test := false
)

lazy val root = (project in file(".")).settings(buildSettings: _*)