name := "VFMOEA-BigData"

version := "1.0"

scalaVersion := "2.10.6"

val jMetalVersion = "5.6"
val sparkVersion = "2.1.0"

mainClass in Compile := Some("Main")

libraryDependencies ++= Seq(
  "org.uma.jmetal" % "jmetal-core" % jMetalVersion ,
  "org.uma.jmetal" % "jmetal-algorithm" % jMetalVersion,
  "info.picocli" % "picocli" % "3.8.0",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion 
 )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}