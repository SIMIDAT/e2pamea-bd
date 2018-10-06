name := "pruebaJMetal"

version := "0.1"

scalaVersion := "2.12.7"

val jMetalVersion = "5.6"

libraryDependencies ++= Seq(
  "org.uma.jmetal" % "jmetal-core" % jMetalVersion,
  "org.uma.jmetal" % "jmetal-algorithm" % jMetalVersion,
  "org.uma.jmetal" % "jmetal-exec" % jMetalVersion,
  "org.uma.jmetal" % "jmetal-problem" % jMetalVersion,
  "nz.ac.waikato.cms.weka" % "weka-stable" % "3.8.0"
)