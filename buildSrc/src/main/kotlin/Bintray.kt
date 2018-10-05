import java.io.File
import java.lang.RuntimeException

object BINTRAY {
  val user = "programingjd"
  fun password(rootProjectDir: File) = File(rootProjectDir, "local.properties").let {
    if (!it.exists()) throw RuntimeException("${it} is missing.")
    val regex = Regex("^\\s*bintrayApiKey\\s*=\\s*(.*)\\s*$")
    val line = it.readLines().findLast { it.matches(regex) } ?:
               throw RuntimeException("bintrayApiKey is not defined in ${it}.")
    regex.find(line)!!.groupValues[1]
  }

}
