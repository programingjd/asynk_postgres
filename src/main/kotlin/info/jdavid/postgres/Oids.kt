package info.jdavid.postgres

enum class Oids(private val oid: kotlin.Int, internal val arrayOf: Oids? = null) {

  Char(18),
  CharArray(1002, Char),
  Boolean(16),
  BooleanArray(1000, Boolean),
  Short(21),
  ShortArray(1005, Short),
  Int(23),
  IntArray(1007, Int),
  Float(700),
  FloatArray(1021, Float),
  Long(20),
  LongArray(1016, Long),
  Double(701),
  DoubleArray(1022, Double),
  BigDecimal(1700),
  BigDecimalArray(1231, BigDecimal),
  Date(1082),
  DateArray(1182, Date),
  Timestamp(1114),
  TimestampArray(1115, Timestamp),
  TimestampZ(1184),
  TimestampZArray(1185, TimestampZ),
  ByteArray(17),
  ByteArrayArray(1001, ByteArray),
  Bit(1560),
  BitArray(1561, Bit),
  VarBit(1562),
  VarBitArray(1563, VarBit),
  Void(2278),
  Json(114),
//JsonB(3802),
  Text(25),
  TextArray(1009, Text),
  VarChar(1043),
  VarCharArray(1015, VarChar),
  Oid(26),
  OidArray(1028, Oid),
  BpChar(1042),
  BpCharArray(1014, BpChar),
  Name(19),
  NameArray(1003, Name),
  UUID(2950),
  UUIDArray(2951, UUID),
  XML(142),
  XMLArray(143, XML),
  Point(600);
//Box(603)

  companion object {
    private val map = mutableMapOf<kotlin.Int, Oids>().apply {
      values().forEach { put(it.oid, it) }
    }
    internal fun all(): Collection<Oids> = map.values
    internal fun fromOid(oid: kotlin.Int): Oids {
      return map[oid] ?: throw RuntimeException("Unsupported type: oid=${oid}")
    }
  }

}
