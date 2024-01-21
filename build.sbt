val scala3Version = "3.2.2"
val projectName = "platcluster"
val projectVersion = "0.1.0-SNAPSHOT"

lazy val root = project
  .in(file("."))
  .settings(
    name := projectName,
    version := projectVersion,
    scalaVersion := scala3Version,
    assembly / assemblyJarName := s"${projectName}-${projectVersion}.jar",

    libraryDependencies ++= Seq(
        "org.scalameta" %% "munit" % "0.7.29" % Test,
        "io.github.flxj" %% "platdb" % "0.12.0"
    )
  )

scalacOptions ++= Seq("-encoding", "utf8")
javacOptions ++= Seq("-encoding", "utf8")
Compile / doc / scalacOptions ++= Seq("-siteroot", "docs")
Compile / doc / scalacOptions ++= Seq("-project", "platdb")


// akka
resolvers += "Akka library repository".at("https://repo.akka.io/maven")

val AkkaVersion = "2.8.3"
val AkkaHttpVersion = "10.5.0"
libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
      "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
      "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % AkkaHttpVersion
    )

libraryDependencies ++=Seq(
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
      "ch.qos.logback" % "logback-classic" % "1.2.10",
      "com.typesafe" % "config" % "1.4.3"
)

// http4s
val http4sVersion = "0.22.15"
val circeVersion = "0.14.6"

libraryDependencies ++= Seq(
  "org.http4s" %% "http4s-blaze-server" % http4sVersion,
  "org.http4s" %% "http4s-blaze-client" % http4sVersion,
  "org.http4s" %% "http4s-dsl" % http4sVersion,
  "org.http4s" %% "http4s-circe" % http4sVersion,
  // Optional for auto-derivation of JSON codecs
  "io.circe" %% "circe-generic" % circeVersion,
  // Optional for string interpolation to JSON model
  "io.circe" %% "circe-literal" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion
)

//addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

// json
libraryDependencies += "com.lihaoyi" %% "upickle" % "3.1.0"


// build
ThisBuild / organization := "io.github.flxj"
ThisBuild / organizationName := "platcluster"
ThisBuild / organizationHomepage := Some(url("https://github.com/flxj/platcluster"))
ThisBuild / versionScheme := Some("early-semver")

name := projectName

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/flxj/platcluster"),
    "scm:git@github.com:flxj/platcluster.git"

  )
)
ThisBuild / developers := List(
  Developer(
    id = "flxj",
    name = "flxj",
    email = "your@email",
    url = url("https://github.com/flxj")
  )
)

ThisBuild / description := "paltcluster is a key-value storage base on raft algorithem,implementd by scala."
ThisBuild / licenses := List(
  "Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt")
)
ThisBuild / homepage := Some(url("https://github.com/flxj/platcluster"))

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  // For accounts created after Feb 2021:
  val nexus = "https://s01.oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publishMavenStyle := true
