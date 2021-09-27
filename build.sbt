ThisBuild / scalaVersion := "2.12.10"
ThisBuild / autoCompilerPlugins := true
ThisBuild / assemblyJarName := "jhyoo1220.jar"

val emrVersion = "6.3.0"
val sparkVersion = "3.1.1"
val javaVersion = "1.8.0_282"
val sbtVersion = "1.5.2"
val assemblyVersion = "0.15.0"

lazy val batch = (project in file("."))
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-yarn" % sparkVersion % "provided",
      "org.scalatest" %% "scalatest" % "3.2.8"),
    resolvers ++= Seq(
      "EMR Repository" at s"https://s3.ap-northeast-2.amazonaws.com/ap-northeast-2-emr-artifacts/$emrVersion/repos/maven/",
      Resolver.sonatypeRepo("public"),
      Resolver.typesafeRepo("releases")))

baseAssemblySettings

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _ @_) => MergeStrategy.discard
  case _ => MergeStrategy.last
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
