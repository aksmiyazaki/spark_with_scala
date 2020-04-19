import sbt.{ExclusionRule, _}


object Dependencies {
  val sparkVersion = "2.4.4"

  val embeddedDependencies = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion exclude("org.scalatest", "scalatest"),
    "org.apache.spark" %% "spark-sql" % sparkVersion
  )

  val rootDependencies = (
    embeddedDependencies
    ).map(_ excludeAll ExclusionRule(organization = "javax.mail"))
}
