lazy val root = (project in file(".")).
    settings(
        name := "ar-model-consine-similarity",
        version := "1.0",
        scalaVersion := "2.10.4",
        mainClass in Compile := Some("org.venraas.spark.CosineSim")
    )

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.5.2" % "provided",
    "org.apache.spark" % "spark-mllib_2.10" % "1.5.2" % "provided",
    "com.github.scopt" % "scopt_2.10" % "3.2.0"
)

// META-INF discarding
//mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
//    {
//        case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//        case x => MergeStrategy.first
//    }
//}

