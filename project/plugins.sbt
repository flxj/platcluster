addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.5")  

resolvers += "Akka library repository".at("https://repo.akka.io/maven")

addSbtPlugin("com.lightbend.akka.grpc" % "sbt-akka-grpc" % "2.3.0")

//addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")

//libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.11"