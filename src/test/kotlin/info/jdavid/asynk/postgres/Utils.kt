package info.jdavid.asynk.postgres

import java.io.File
import java.util.*

object Utils {
  internal fun properties(): Properties {
    var dir: File? = File(this::class.java.protectionDomain.codeSource.location.toURI())
    while (dir != null) {
      if (File(dir, ".git").exists()) break
      dir = dir.parentFile
    }
    val props = Properties()
    if (dir != null) {
      val file = File(dir, "local.properties")
      if (file.exists()) {
        props.load(file.reader())
      }
    }
    return props
  }
}
