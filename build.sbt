

ThisBuild / scalaVersion := "2.13.2"
ThisBuild / name := "promotions"
ThisBuild / organization := "com.craftcodehouse"
ThisBuild / version := "1.0"

libraryDependencies += "org.typelevel" %% "cats-core" % "2.1.1"


val kafkaVersion = "5.4.0-ccs"
//libraryDependencies += "org.apache.kafka" %% "kafka" % kafkaVersion
ThisBuild / libraryDependencies += "org.apache.kafka" % "kafka-streams" % kafkaVersion
ThisBuild / libraryDependencies += "org.apache.kafka" % "kafka-clients" % kafkaVersion
ThisBuild / libraryDependencies += "org.apache.avro" % "avro" % "1.9.0"
ThisBuild / libraryDependencies += "org.apache.avro" % "avro-maven-plugin" % "1.9.0"

ThisBuild / resolvers += "confluent" at "https://packages.confluent.io/maven/"
// note that 5.4 of this is kafka/kafka-streams 2.4.If you change the above, may need to change this
// or may be able to change the above to libraryDependencies += "org.apache.kafka" % "kafka-streams" % "5.4.0"
// now done the latter as version conflicts were a problem
ThisBuild / libraryDependencies += "io.confluent" % "kafka-streams-avro-serde" % "5.4.0"

ThisBuild / libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.11.0"
ThisBuild / libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.10.1"



//lazy val root = (project in file(".")).
//  settings(
//    inThisBuild(List(
//      organization := "ch.epfl.scala",
//      scalaVersion := "2.12.8"
//    )),
//    name := "hello-world"
//  )



lazy val promotions = (project in file("promotions"))
  .settings(
    mainClass in (Compile, run) := Some("com.craftcodehouse.promotions.accumulator.PaymentAccumulator") ,
//    resourceDirectory in Compile := file(".") / "./src/main/resources",
//    resourceDirectory in Runtime := file(".") / "./src/main/resources",
    // other settings
  )


lazy val ims = (project in file("ims"))
  .settings(
    mainClass in (Compile, run) := Some("com.craftcodehouse.ims.IMS")
    // other settings
  )


//lazy val util = (project in file("util"))
//  .settings(
//    // other settings
//  )



// To learn more about multi-project builds, head over to the official sbt
// documentation at http://www.scala-sbt.org/documentation.html
