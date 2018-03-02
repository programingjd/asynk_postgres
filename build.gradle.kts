import com.jfrog.bintray.gradle.BintrayExtension
import java.io.FileInputStream
import java.io.FileWriter
import org.cyberneko.html.parsers.DOMParser
import org.jetbrains.kotlin.config.CompilerConfiguration
import org.jetbrains.kotlin.gradle.dsl.Coroutines
import org.jetbrains.kotlin.gradle.dsl.KotlinJvmCompile
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.w3c.dom.Node
import org.xml.sax.InputSource
import javax.xml.xpath.XPathConstants
import javax.xml.xpath.XPathFactory

buildscript {
  repositories {
    jcenter()
  }
}

plugins {
  kotlin("jvm") version "1.2.30"
  `maven-publish`
  id("com.jfrog.bintray") version "1.8.0"
}

group = "info.jdavid.postgres"
version = "1.0.0.6"

repositories {
  jcenter()
}

dependencies {
  compile(kotlin("stdlib-jdk8"))
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:0.22.3")
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-nio:0.22.3")
  implementation("info.jdavid.sql:sql:1.0.0.8")
  implementation("org.slf4j:slf4j-api:1.7.25")
  testImplementation("junit:junit:4.12")
  testImplementation("com.fasterxml.jackson.core:jackson-databind:2.9.4")
  testRuntime("org.slf4j:slf4j-jdk14:1.7.25")
}

kotlin {
  experimental.coroutines = Coroutines.ENABLE
}

val sourcesJar by tasks.creating(Jar::class) {
  classifier = "sources"
  from(java.sourceSets["main"].allSource)
}

val javadocJar by tasks.creating(Jar::class) {
  classifier = "javadoc"
  from(java.docsDir)
}

tasks.withType(KotlinJvmCompile::class.java).all {
  kotlinOptions {
    jvmTarget = "1.8"
  }
}

val jar: Jar by tasks
jar.apply {
  manifest {
    attributes["Sealed"] = true
  }
}

publishing {
  repositories {
    maven {
      url = uri("${buildDir}/repo")
    }
  }
  (publications) {
    "mavenJava"(MavenPublication::class) {
      from(components["java"])
      artifact(sourcesJar)
      artifact(javadocJar)
    }
  }
}

bintray {
  user = "programingjd"
  key = {
    "bintrayApiKey".let { key: String ->
      File("local.properties").readLines().findLast {
        it.startsWith("${key}=")
      }?.substring(key.length + 1)
    }
  }()
  //dryRun = true
  publish = true
  setPublications("mavenJava")
  pkg(delegateClosureOf<BintrayExtension.PackageConfig>{
    repo = "maven"
    name = "${project.group}"
    websiteUrl = "https://github.com/programingjd/postgres"
    issueTrackerUrl = "https://github.com/programingjd/postgres/issues"
    vcsUrl = "https://github.com/programingjd/postgres.git"
    githubRepo = "programingjd/postgres"
    githubReleaseNotesFile = "README.md"
    setLicenses("Apache-2.0")
    setLabels("postgres", "postgresql", "sql", "java", "kotlin", "async", "coroutines")
    publicDownloadNumbers = true
    version(delegateClosureOf<BintrayExtension.VersionConfig> {
      name = "${project.version}"
      mavenCentralSync(delegateClosureOf<BintrayExtension.MavenCentralSyncConfig> {
        sync = false
      })
    })
  })
}

tasks {
  "test" {
    val test = this as Test
    doLast {
      DOMParser().let {
        it.parse(InputSource(FileInputStream(test.reports.html.entryPoint)))
        XPathFactory.newInstance().newXPath().apply {
          val total =
            (
              evaluate("DIV", it.document.getElementById("tests"), XPathConstants.NODE) as Node
            ).textContent.toInt()
          val failed =
            (
              evaluate("DIV", it.document.getElementById("failures"), XPathConstants.NODE) as Node
            ).textContent.toInt()
          val badge = { label: String, text: String, color: String ->
            "https://img.shields.io/badge/_${label}_-${text}-${color}.png?style=flat"
          }
          val color = if (failed == 0) "green" else if (failed < 3) "yellow" else "red"
          File("README.md").apply {
            readLines().mapIndexed { i, line ->
              when (i) {
                0 -> "![jcenter](${badge("jcenter", "${project.version}", "6688ff")}) &#x2003; " +
                     "![jcenter](${badge("Tests", "${total-failed}/${total}", color)})"
                9 -> "[Download](https://bintray.com/artifact/download/programingjd/maven/info/jdavid/postgres/postgres/${project.version}/postgres-${project.version}.jar) the latest jar."
                19 -> "  <version>${project.version}</version>"
                32 -> "  compile 'info.jdavid.postgres:postgres:${project.version}'"
                else -> line
              }
            }.joinToString("\n").let {
              FileWriter(this).apply {
                write(it)
                close()
              }
            }
          }
        }
      }
    }
  }
  "bintrayUpload" {
    dependsOn("check")
  }
}
