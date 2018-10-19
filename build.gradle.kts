@file:Suppress("RemoveCurlyBracesFromTemplate")

import java.io.FileInputStream
import java.io.FileWriter
import com.jfrog.bintray.gradle.BintrayExtension
import org.cyberneko.html.parsers.DOMParser
import org.w3c.dom.Node
import org.xml.sax.InputSource
import javax.xml.xpath.XPathConstants
import javax.xml.xpath.XPathFactory

plugins {
  kotlin("jvm") version KOTLIN.version
  id("org.jetbrains.dokka") version "0.9.17"
  `maven-publish`
  id("com.jfrog.bintray") version "1.8.4"
  id("io.gitlab.arturbosch.detekt").version("1.0.0.RC9.2")
}

group = "info.jdavid.asynk"
version = "${ASYNK.version}.3"

repositories {
  jcenter()
}

dependencies {
  implementation("org.jetbrains.kotlin:kotlin-stdlib:${KOTLIN.version}")
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:${KOTLINX.version}")
  implementation("info.jdavid.asynk:core:${ASYNK.version}")
  implementation("info.jdavid.asynk:sql:${ASYNK.version}")
  implementation("org.slf4j:slf4j-api:1.7.25")
  testImplementation("org.junit.jupiter:junit-jupiter-api:5.3.1")
  testImplementation("org.junit.jupiter:junit-jupiter-params:5.3.1")
  testRuntime("org.junit.jupiter:junit-jupiter-engine:5.3.1")
  testRuntime("org.slf4j:slf4j-jdk14:1.7.25")
  testImplementation("com.fasterxml.jackson.core:jackson-databind:2.9.7")
  testImplementation("org.apache.httpcomponents:httpclient:4.5.6")
}


tasks.compileKotlin {
  kotlinOptions {
    jvmTarget = "1.8"
  }
}
tasks.compileTestKotlin {
  kotlinOptions {
    jvmTarget = "1.8"
  }
}

tasks.jar {
  dependsOn("detekt")
}

tasks.dokka {
  outputFormat = "javadoc"
  includeNonPublic = false
  skipEmptyPackages = true
  impliedPlatforms = mutableListOf("JVM")
  jdkVersion = 8
  outputDirectory = "${buildDir}/javadoc"
}

tasks.javadoc {
  dependsOn("dokka")
}

tasks.test {
  @Suppress("UnstableApiUsage") useJUnitPlatform()
  testLogging {
    events("passed", "skipped", "failed")
  }
}

tasks.jar {
  manifest {
    attributes["Sealed"] = true
  }
}

val sourcesJar by tasks.registering(Jar::class) {
  classifier = "sources"
  from (sourceSets["main"].allSource)
}

val javadocJar by tasks.registering(Jar::class) {
  classifier = "javadoc"
  from(tasks.dokka.get().outputDirectory)
  dependsOn("javadoc")
}

publishing {
  repositories {
    maven {
      url = uri("${buildDir}/repo")
    }
    publications {
      register("mavenJava", MavenPublication::class) {
        @Suppress("UnstableApiUsage") from(components["java"])
        artifact(sourcesJar.get())
        artifact(javadocJar.get())
      }
    }
  }
}

bintray {
  user = BINTRAY.user
  key = BINTRAY.password(rootProject.projectDir)
  publish = true
  setPublications(*publishing.publications.names.toTypedArray())
  pkg(delegateClosureOf<BintrayExtension.PackageConfig>{
    repo = "maven"
    name = "${project.group}.${project.name}"
    websiteUrl = "https://github.com/${BINTRAY.user}/asynk_${project.name}"
    issueTrackerUrl = "https://github.com/${BINTRAY.user}/asynk_${project.name}/issues"
    vcsUrl = "https://github.com/${BINTRAY.user}/asynk_${project.name}.git"
    githubRepo = "${BINTRAY.user}/asynk_${project.name}"
    githubReleaseNotesFile = "README.md"
    setLicenses("Apache-2.0")
    setLabels("asynk", "java", "kotlin", "async", "coroutines", "suspend", "nio", "nio2")
    publicDownloadNumbers = true
    version(delegateClosureOf<BintrayExtension.VersionConfig> {
      name = "${project.version}"
      mavenCentralSync(delegateClosureOf<BintrayExtension.MavenCentralSyncConfig> {
        sync = false
      })
    })
  })
}

tasks.bintrayUpload {
  dependsOn("check")
}

detekt {
  config = files("detekt.yml")
}

tasks.test {
  doLast {
    DOMParser().also { parser ->
      parser.parse(InputSource(FileInputStream(reports.html.entryPoint)))
      XPathFactory.newInstance().newXPath().apply {
        val total =
          (evaluate("DIV", parser.document.getElementById("tests"), XPathConstants.NODE) as Node).
            textContent.toInt()
        val failed =
          (evaluate("DIV", parser.document.getElementById("failures"), XPathConstants.NODE) as Node).
            textContent.toInt()
        val badge = { label: String, text: String, color: String ->
          "https://img.shields.io/badge/_${label}_-${text}-${color}.png?style=flat"
        }
        val color = if (failed == 0) "green" else if (failed < 3) "yellow" else "red"
        File("README.md").apply {
          readLines().mapIndexed { i, line ->
            when (i) {
              0 -> "![jcenter](${badge("jcenter", "${project.version}", "6688ff")}) &#x2003; " +
                   "![jcenter](${badge("Tests", "${total-failed}/${total}", color)})"
              9 -> "[Download](https://bintray.com/artifact/download/programingjd/maven/info/jdavid/asynk/postgres/${project.version}/postgres-${project.version}.jar) the latest jar."
              19 -> "  <version>${project.version}</version>"
              32 -> "  compile 'info.jdavid.asynk:postgres:${project.version}'"
              else -> line
            }
          }.joinToString("\n").also { line ->
            FileWriter(this).use { it.write(line) }
          }
        }
      }
    }
  }
}
