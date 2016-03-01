
lazy val branch = "git rev-parse --abbrev-ref HEAD".!!.trim
lazy val commit = "git rev-parse --short HEAD".!!.trim
lazy val author = s"git show --format=%an -s $commit".!!.trim
lazy val buildDate = (new java.text.SimpleDateFormat("yyyyMMdd"))
  .format(new java.util.Date())
lazy val appVersion = "%s-%s-%s".format(branch, buildDate, commit)

lazy val commonSettings = Seq(
  scalaVersion := "2.11.7",
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
  "net.sf.ehcache" % "ehcache" % "2.10.1",
  "org.quartz-scheduler" % "quartz" % "2.2.2",
  "org.reflections" % "reflections" % "0.9.10" notTransitive (),
  "junit"             % "junit"           % "4.12"  % "test",
  "com.novocode"      % "junit-interface" % "0.11"  % "test",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.6.3",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.3",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.6.3",
  "log4j" % "log4j" % "1.2.17",
  "org.webjars" %% "webjars-play" % "2.4.0",
  "org.webjars" % "bootstrap" % "3.3.6",
  "org.webjars" % "typeaheadjs" % "0.11.1",
  "org.webjars" % "handlebars" % "4.0.2",
  "org.webjars" % "jquery" % "2.2.0",
  "org.webjars" % "font-awesome" % "4.5.0",
  "org.webjars" % "html5shiv" % "3.7.2",
  "org.webjars" % "requirejs" % "2.1.15",
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
  .settings(
  name := "ixcurator-core",
    unmanagedBase <<= baseDirectory { base => base / "../../lib" },
    libraryDependencies ++= commonDependencies,
    javacOptions ++= javaBuildOptions
).dependsOn(buildinfo).aggregate(buildinfo)


// Play provides two styles of routers, one expects its actions to be injected, the
// other, legacy style, accesses its actions statically.
routesGenerator := InjectedRoutesGenerator
