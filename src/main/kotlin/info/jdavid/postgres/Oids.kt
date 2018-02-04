package info.jdavid.postgres

sealed class Oids(internal val oid: kotlin.Int) {
  init { map.put(oid, this) }

  interface Array<T: Oids>

  object Char: Oids(18)
  object CharArray: Array<Char>, Oids(1002)
  object Boolean: Oids(16)
  object BooleanArray: Array<Boolean>, Oids(1000)
  object Short: Oids(21)
  object ShortArray: Array<Short>, Oids(1005)
  object Int: Oids(23)
  object IntArray: Array<Int>, Oids(1007)
  object Float: Oids(700)
  object FloatArray: Array<Float>, Oids(1021)
  object Long: Oids(20)
  object LongArray: Array<Long>, Oids(1016)
  object Double: Oids(701)
  object DoubleArray: Array<Double>, Oids(1022)
  object BigDecimal: Oids(1700)
  object BigDecimalArray: Array<BigDecimal>, Oids(1231)
  object Date: Oids(1082)
  object DateArray: Array<Date>, Oids(1182)
  object Timestamp: Oids(1114)
  object TimestampArray: Array<Timestamp>, Oids(1115)
  object TimestampZ: Oids(1184)
  object TimestampZArray: Array<TimestampZ>, Oids(1185)
  object ByteArray: Oids(17)
  object ByteArrayArray: Array<ByteArray>, Oids(1001)
  object Bit: Oids(1560)
  object BitArray: Array<Bit>, Oids(1561)
  object VarBit: Oids(1562)
  object VarBitArray: Array<VarBit>, Oids(1563)
  object Void: Oids(2278)
  object Json: Oids(114)
  object JsonB: Oids(3802)
  object Text: Oids(25)
  object TextArray: Array<Text>, Oids(1009)
  object VarChar: Oids(1043)
  object VarCharArray: Array<VarChar>, Oids(1015)
  object Oid: Oids(26)
  object OidArray: Array<Oid>, Oids(1028)
  object BpChar: Oids(1042)
  object BpCharArray: Array<BpChar>, Oids(1014)
  object Name: Oids(19)
  object NameArray: Array<Name>, Oids(1003)
  object UUID: Oids(2950)
  object UUIDArray: Array<UUID>, Oids(2951)
  object XML: Oids(142)
  object XMLArray: Array<XML>, Oids(143)
  object Point: Oids(600)
  object Box: Oids(603)

  companion object {
    private val map = mutableMapOf<kotlin.Int, Oids>()
    internal fun all(): Collection<Oids> = map.values
    internal fun fromOid(oid: kotlin.Int) = map[oid] ?: throw RuntimeException("Unsupported type: oid=${oid}")
  }

}
