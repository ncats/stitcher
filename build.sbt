
lazy val branch = "git rev-parse --abbrev-ref HEAD".!!.trim
lazy val commit = "git rev-parse --short HEAD".!!.trim
lazy val author = s"git show --format=%an -s $commit".!!.trim
lazy val buildDate = (new java.text.SimpleDateFormat("yyyyMMdd"))
  .format(new java.util.Date())
lazy val appVersion = "%s-%s-%s".format(branch, buildDate, commit)

lazy val commonSettings = Seq(
  scalaVersion := "2.11.8",
  version := appVersion
)

lazy val javaBuildOptions = Seq(
  "-encoding", "UTF-8"
    //,"-Xlint:-options"
    //,"-Xlint:deprecation"
)

lazy val commonDependencies = Seq(
  javaJdbc,
  cache,
  javaWs,
  "org.neo4j" % "neo4j" % "2.3.2",
  "org.apache.lucene" % "lucene-facet" % "3.6.2",
  "org.apache.lucene" % "lucene-highlighter" % "3.6.2",
  "org.apache.lucene" % "lucene-queryparser" % "3.6.2",
  "org.apache.lucene" % "lucene-queries" % "3.6.2",
  "org.apache.lucene" % "lucene-analyzers" % "3.6.2",
  "org.quartz-scheduler" % "quartz" % "2.2.2",
  "org.reflections" % "reflections" % "0.9.10" notTransitive (),
  "junit"             % "junit"           % "4.12"  % "test",
  "com.novocode"      % "junit-interface" % "0.11"  % "test",
  "log4j" % "log4j" % "1.2.17",
  "org.webjars" %% "webjars-play" % "2.5.0",
  "org.webjars" % "bootstrap" % "3.3.6",
  "org.webjars" % "typeaheadjs" % "0.11.1",
  "org.webjars" % "handlebars" % "4.0.2",
  "org.webjars" % "jquery" % "2.2.0",
  "org.webjars" % "font-awesome" % "4.5.0",
  "org.webjars" % "html5shiv" % "3.7.3",
  "org.webjars" % "requirejs" % "2.1.22",
  "org.webjars" % "respond" % "1.4.2",
  "org.webjars" % "morrisjs" % "0.5.1"
)

lazy val root = (project in file("."))
  .enablePlugins(PlayJava, PlayEbean)
  .settings(commonSettings: _*)
  .settings(name := """ixcurator""")
  .settings(
  libraryDependencies ++= commonDependencies
).dependsOn(core).aggregate(core, buildinfo)

lazy val buildinfo = (project in file("modules/build"))
  .settings(commonSettings: _*)
  .settings(
  name := "ixcurator-buildinfo",
    sourceGenerators in Compile <+= sourceManaged in Compile map { dir =>
      val file = dir / "BuildInfo.java"
      IO.write(file, """
package ix;
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

lazy val core = (project in file("modules/core"))
  .settings(commonSettings: _*)
  .settings(name := "ixcurator-core",
    unmanagedBase <<= baseDirectory { base => base / "../../lib" },
    cleanFiles <<= baseDirectory {
      base => ((base / "../../") ** "_ix*.db").get
    },
    libraryDependencies ++= commonDependencies,
    libraryDependencies += "com.typesafe" % "config" % "1.2.0",
    javacOptions ++= javaBuildOptions
).dependsOn(buildinfo).aggregate(buildinfo)
