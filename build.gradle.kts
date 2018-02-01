import com.jfrog.bintray.gradle.BintrayExtension
import org.jetbrains.kotlin.config.CompilerConfiguration
import org.jetbrains.kotlin.gradle.dsl.Coroutines
import org.jetbrains.kotlin.gradle.dsl.KotlinJvmCompile
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

buildscript {
  repositories {
    jcenter()
  }
}

plugins {
  kotlin("jvm") version "1.2.21"
  `maven-publish`
  id("com.jfrog.bintray") version "1.7.3"
}

group = "info.jdavid.postgres"
version = "1.0.0"

repositories {
  jcenter()
}

dependencies {
  compile(kotlin("stdlib-jdk8"))
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:0.22.1")
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-nio:0.22.1")
  implementation("org.slf4j:slf4j-api:1.7.25")
  testImplementation("junit:junit:4.12")
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
  dryRun = true
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

