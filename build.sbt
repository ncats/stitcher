
lazy val branch = "git rev-parse --abbrev-ref HEAD".!!.trim
lazy val commit = "git rev-parse --short HEAD".!!.trim
lazy val author = s"git show --format=%an -s $commit".!!.trim
lazy val buildDate = (new java.text.SimpleDateFormat("yyyyMMdd"))
  .format(new java.util.Date())
lazy val appVersion = "%s-%s-%s".format(branch, buildDate, commit)

lazy val commonSettings = Seq(
  scalaVersion := "2.11.11",
  version := appVersion
)

lazy val javaBuildOptions = Seq(
  "-encoding", "UTF-8"
    //,"-Xlint:-options"
    //,"-Xlint:deprecation"
)

lazy val commonDependencies = Seq(
  guice,
  javaJdbc,
  ehcache,
  javaWs,
  "com.typesafe.play" %% "play-json" % "2.6.0",
  "com.h2database" % "h2" % "1.4.193",
  "org.neo4j" % "neo4j" % "3.2.14",
  "org.neo4j" % "neo4j-bolt" % "3.2.14",
  "org.neo4j.driver" % "neo4j-java-driver" % "4.0.1",
  //"org.neo4j.app" % "neo4j-server" % "3.2.14",
  "org.apache.commons" % "commons-text" % "1.6",  
  "org.apache.lucene" % "lucene-core" % "5.5.0",
  "org.apache.lucene" % "lucene-facet" % "5.5.0",
  "org.apache.lucene" % "lucene-analyzers-common" % "5.5.0",
  "org.apache.lucene" % "lucene-queryparser" % "5.5.0",
  "org.apache.lucene" % "lucene-queries" % "5.5.0",
  "org.apache.lucene" % "lucene-highlighter" % "5.5.0",
  "org.apache.lucene" % "lucene-suggest" % "5.5.0",
  "org.quartz-scheduler" % "quartz" % "2.2.2",
  "org.reflections" % "reflections" % "0.9.10",// notTransitive (),
  "junit"             % "junit"           % "4.12"  % "test",
  "com.novocode"      % "junit-interface" % "0.11"  % "test",
  "org.testng" % "testng" % "7.1.0" % "test",
  "org.apache.logging.log4j" % "log4j-api" % "2.16.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.16.0",
  "org.webjars" %% "webjars-play" % "2.6.1",
  "org.webjars" % "bootstrap" % "3.3.6",
  "org.webjars" % "typeaheadjs" % "0.11.1",
  "org.webjars" % "handlebars" % "4.0.2",
  "org.webjars" % "jquery" % "2.2.0",
  "org.webjars" % "font-awesome" % "4.5.0",
  "org.webjars" % "html5shiv" % "3.7.3",
  "org.webjars" % "requirejs" % "2.1.22",
  "org.webjars" % "respond" % "1.4.2",
  "org.webjars" % "morrisjs" % "0.5.1",
  "org.freehep" % "freehep-graphicsbase" % "2.4",
  "org.freehep" % "freehep-vectorgraphics" % "2.4",
  "org.freehep" % "freehep-graphicsio" % "2.4",
  "org.freehep" % "freehep-graphicsio-svg" % "2.4",
  "org.freehep" % "freehep-graphics2d" % "2.4",
  "com.github.fge" % "json-patch" % "1.9",
  "org.apache.jena" % "apache-jena-libs" % "3.14.0",
  "mysql" % "mysql-connector-java" % "5.1.31",
  "com.microsoft.sqlserver" % "mssql-jdbc" % "7.2.0.jre8"
)

lazy val root = (project in file("."))
  .enablePlugins(PlayJava, PlayEbean, JavaAppPackaging, UniversalPlugin)
  .settings(commonSettings: _*)
  .settings(name := """ncats-stitcher""")
  .settings(
      libraryDependencies ++= commonDependencies,
      mappings in Universal ++= {
        val dataDir = baseDirectory.value / "stitcher-inputs"
        Seq(
          dataDir / "combined_withdrawn_shortage_drugs.txt" -> "data/combined_withdrawn_shortage_drugs.txt",
          dataDir / "dev_status_logic.txt" -> "data/dev_status_logic.txt"
        )
      }
    ).dependsOn(stitcher).aggregate(stitcher, buildinfo)

lazy val buildinfo = (project in file("modules/build"))
  .settings(commonSettings: _*)
  .settings(
  name := "stitcher-buildinfo",
    sourceGenerators in Compile += sourceManaged in Compile map { dir =>
      val file = dir / "BuildInfo.java"
      IO.write(file, """
package ncats.stitcher;
public class BuildInfo { 
   public static final String BRANCH = "%s";
   public static final String DATE = "%s";
   public static final String COMMIT = "%s";
   public static final String TIME = "%s";
   public static final String AUTHOR = "%s";
}
""".format(branch, buildDate, commit, new java.util.Date(), author))
      Seq(file)
    }
)

lazy val stitcher = (project in file("modules/stitcher"))
  .settings(commonSettings: _*)
  .settings(name := "stitcher-core",
    unmanagedBase := baseDirectory { base => base / "../../lib" }.value,
    cleanFiles := baseDirectory {
      base => ((base / "../../") ** "_ix*.db").get ++ (base / "target").get
    }.value,
    libraryDependencies ++= commonDependencies,
    libraryDependencies += "com.typesafe" % "config" % "1.2.0",
    javacOptions ++= javaBuildOptions
).dependsOn(buildinfo).aggregate(buildinfo)

lazy val dailymed = (project in file("modules/dailymed"))
  .settings(commonSettings: _*)
  .settings(name := "stitcher-dailymed",
    libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.1",
    libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.1",
    javacOptions ++= javaBuildOptions
)

lazy val disease = (project in file("modules/disease"))
  .settings(commonSettings: _*)
  .settings(name := "stitcher-disease",
    javacOptions ++= javaBuildOptions
  ).dependsOn(stitcher).aggregate(stitcher)


fork in run := true
