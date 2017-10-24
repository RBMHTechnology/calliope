import de.heikoseeberger.sbtheader.{AutomateHeaderPlugin, HeaderPlugin}
import de.heikoseeberger.sbtheader.license.Apache2_0
import sbtprotobuf.ProtobufPlugin

lazy val commonSettings = Seq(
  organization := "com.rbmhtechnology",
  name := "calliope",
  version := "0.3.0-SNAPSHOT",
  scalaVersion := "2.11.11",
  crossScalaVersions := Seq("2.11.11", "2.12.1")
)

scalacOptions in (Compile, doc) ++= {
  scalaVersion.value match {
    case "2.12.1" => "-no-java-comments" :: Nil
    case _ => Nil
  }
}

lazy val testSettings = Defaults.itSettings ++ Seq(
  parallelExecution in IntegrationTest := false,
  fork in IntegrationTest := true
)

lazy val headerSettings: Seq[Setting[_]] = {
  val header = Apache2_0("2015 - 2017", "Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.")

  Seq(headers := Map("scala" -> header, "java"  -> header)) ++
    HeaderPlugin.settingsFor(IntegrationTest) ++
    AutomateHeaderPlugin.automateFor(IntegrationTest)
}

lazy val protocSettings: Seq[Setting[_]] = ProtobufPlugin.protobufSettings ++ Seq(
  version in ProtobufPlugin.protobufConfig := Version.Protobuf,
  ProtobufPlugin.runProtoc in ProtobufPlugin.protobufConfig := (args => com.github.os72.protocjar.Protoc.runProtoc("-v320" +: args.toArray))
)

lazy val dependencies = Seq(
  "com.typesafe.akka"       %% "akka-stream"              % Version.Akka ,
  "com.typesafe.akka"       %% "akka-stream-kafka"        % "0.13",
  "org.apache.kafka"        %  "kafka-clients"            % Version.Kafka,
  "org.apache.kafka"        %  "kafka-streams"            % Version.Kafka,
  "org.scala-lang.modules"  %% "scala-java8-compat"       % "0.8.0",
  "io.vavr"                 %  "vavr"                     % "0.9.1",

  "org.scalatest"           %% "scalatest"                % "3.0.1"      % "it,test",
  "com.typesafe.akka"       %% "akka-stream-testkit"      % Version.Akka % "it,test",
  "com.typesafe.akka"       %% "akka-testkit"             % Version.Akka % "it,test",
  "net.manub"               %% "scalatest-embedded-kafka" % "0.11.0"     % "it",

  "com.novocode"            % "junit-interface"           % "0.11"       % "it,test",
  "org.hamcrest"            % "java-hamcrest"             % "2.0.0.0"    % "it,test",
  "info.batey.kafka"        % "kafka-unit"                % "0.7"        % "it"         excludeAll
    ExclusionRule(organization = "org.apache.kafka", name = "kafka_2.11"),

  "com.google.protobuf"     %  "protobuf-java"            % Version.Protobuf
)

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(commonSettings: _*)
  .settings(testSettings)
  .settings(headerSettings)
  .settings(protocSettings: _*)
  .settings(libraryDependencies ++= dependencies)
  .enablePlugins(HeaderPlugin, AutomateHeaderPlugin)
