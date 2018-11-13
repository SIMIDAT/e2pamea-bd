name := "pruebaJMetal"

version := "0.1"

scalaVersion := "2.12.6"

val jMetalVersion = "5.6"
val sparkVersion = "2.4.0"

mainClass in Compile := Some("Main")

libraryDependencies ++= Seq(
  "org.uma.jmetal" % "jmetal-core" % jMetalVersion,
  "org.uma.jmetal" % "jmetal-algorithm" % jMetalVersion,
  "org.uma.jmetal" % "jmetal-exec" % jMetalVersion,
  "org.uma.jmetal" % "jmetal-problem" % jMetalVersion,
  "nz.ac.waikato.cms.weka" % "weka-stable" % "3.8.0",
  "commons-cli" % "commons-cli" % "1.4",             // For parsing the parameters in the CLI
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
 )