import mill._, scalalib._, publish._
import coursier.MavenRepository

val crossMatrix = for {
  scala  <- Seq("2.11.8", "2.12.4")
  spark <- Seq("2.3.0", "2.4.0")
  if !(scala >= "2.12.0" && spark < "2.4.0")
} yield (scala, spark)

object `spark-constraints` extends Cross[SparkConstraintModule](crossMatrix: _*)

class SparkConstraintModule(val crossScalaVersion: String, val crossSparkVersion: String)
extends CrossScalaModule with PublishModule {
  def publishVersion = "0.1.0"

  def artifactName = "spark-constraints"

  override def pomSettings = PomSettings(
    description = "A module for validating extra constraints on spark datasets.",
    organization = "com.nikvanderhoof",
    url = "https://www.github.com/nvander1/spark-constraints",
    licenses = Seq(License.MIT),
    versionControl = VersionControl.github("nvander1", "spark-constraints"),
    developers = Seq(
      Developer("nvander1", "Nikolas Vanderhoof", "https://www.github.com/nvander1")
    )
  )

  def repositories = super.repositories ++
    Seq(MavenRepository("https://dl.bintray.com/spark-packages/maven"))

  def compileIvyDeps = Agg(ivy"org.apache.spark::spark-sql:${crossSparkVersion}")


  object test extends Tests {
    val majorMinorVersion = crossScalaVersion.split("\\.").dropRight(1).mkString(".")
    def ivyDeps = Agg(
      ivy"com.lihaoyi::utest:0.6.3",
      ivy"org.apache.spark::spark-sql:${crossSparkVersion}",
      ivy"MrPowers:spark-fast-tests:0.17.1-s_${majorMinorVersion}",
      ivy"mrpowers:spark-daria:0.26.1-s_${majorMinorVersion}"
    )
    def testFrameworks = Seq("utest.runner.Framework")
  }
}
