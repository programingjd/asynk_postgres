![jcenter](https://img.shields.io/badge/_jcenter_-1.0.0.6-6688ff.png?style=flat) &#x2003; ![jcenter](https://img.shields.io/badge/_Tests_-14/14-green.png?style=flat)
# postgres
A Postgres async client with suspend functions for kotlin coroutines.

## Download ##

The maven artifacts are on [Bintray](https://bintray.com/programingjd/maven/info.jdavid.postgres/view)
and [jcenter](https://bintray.com/search?query=info.jdavid.postgres).

[Download](https://bintray.com/artifact/download/programingjd/maven/info/jdavid/postgres/postgres/1.0.0.6/postgres-1.0.0.6.jar) the latest jar.

__Maven__

Include [those settings](https://bintray.com/repo/downloadMavenRepoSettingsFile/downloadSettings?repoPath=%2Fbintray%2Fjcenter)
 to be able to resolve jcenter artifacts.
```
<dependency>
  <groupId>info.jdavid.postgres</groupId>
  <artifactId>postgres</artifactId>
  <version>1.0.0.6</version>
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
  compile 'info.jdavid.postgres:postgres:1.0.0.6'
}
```