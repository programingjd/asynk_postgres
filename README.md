![jcenter](https://img.shields.io/badge/_jcenter_-0.0.0.2-6688ff.png?style=flat) &#x2003; ![jcenter](https://img.shields.io/badge/_Tests_-27/27-green.png?style=flat)
# Asynk POSTGRES
A Postgres async client with suspend functions for kotlin coroutines.

## Download ##

The maven artifacts are on [Bintray](https://bintray.com/programingjd/maven/info.jdavid.asynk.postgres/view)
and [jcenter](https://bintray.com/search?query=info.jdavid.asynk.postgres).

[Download](https://bintray.com/artifact/download/programingjd/maven/info/jdavid/asynk/postgres/0.0.0.2/postgres-0.0.0.2.jar) the latest jar.

__Maven__

Include [those settings](https://bintray.com/repo/downloadMavenRepoSettingsFile/downloadSettings?repoPath=%2Fbintray%2Fjcenter)
 to be able to resolve jcenter artifacts.
```
<dependency>
  <groupId>info.jdavid.asynk</groupId>
  <artifactId>postgres</artifactId>
  <version>0.0.0.2</version>
</dependency>
```
__Gradle__

Add jcenter to the list of maven repositories.
```
repositories {
  jcenter()
}
```
```
dependencies {
  compile 'info.jdavid.asynk:postgres:0.0.0.2'
}
```