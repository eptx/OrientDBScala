name := "OrientDBScala"

version := "0.1"

scalaVersion := "2.9.1"

resolvers ++= Seq("Orient Technologies Maven2 Repository" at "http://www.orientechnologies.com/listing/m2")

libraryDependencies ++= Seq(
    "org.scalatest" % "scalatest_2.9.0" % "1.6.1",
	"com.orientechnologies" % "orient-commons" % "1.0rc7-SNAPSHOT",
    "com.orientechnologies" % "orientdb-core" % "1.0rc7-SNAPSHOT"
)

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")