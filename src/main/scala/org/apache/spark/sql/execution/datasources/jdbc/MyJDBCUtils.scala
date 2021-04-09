// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.execution.datasources.jdbc

import java.sql.{Connection, Driver, DriverManager, PreparedStatement, ResultSet, Types}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.NextIterator

import scala.collection.JavaConverters._

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-04-08.
 */
object MyJDBCUtils extends Logging {

  /**
   * Returns a factory for creating connections to the given JDBC URL.
   *
   * @param options - JDBC options that contains url, table and other information.
   * @throws IllegalArgumentException if the driver could not open a JDBC connection.
   */
  def createConnectionFactory(options: JDBCOptions): () => Connection = {
    val driverClass: String = options.driverClass
    () => {
      DriverRegistry.register(driverClass)
      val driver: Driver = DriverManager.getDrivers.asScala.collectFirst {
        case d: DriverWrapper if d.wrapped.getClass.getCanonicalName == driverClass => d
        case d if d.getClass.getCanonicalName == driverClass => d
      }.getOrElse {
        throw new IllegalStateException(
          s"Did not find registered driver with class $driverClass")
      }
      val connection: Connection = driver.connect(options.url, options.asConnectionProperties)
      require(connection != null,
        s"The driver could not open a JDBC connection. Check the URL: ${options.url}")

      connection
    }
  }

  def resultSetToSparkInternalRows(resultSet: ResultSet,
                                   schema: StructType): Iterator[InternalRow] = {
    new NextIterator[InternalRow] {
      private[this] val rs = resultSet
      private[this] val getters: Array[JDBCValueGetter] = makeGetters(schema)
      private[this] val mutableRow = new SpecificInternalRow(schema.fields.map(x => x.dataType))

      override protected def close(): Unit = {
        try {
          rs.close()
        } catch {
          case e: Exception => logWarning("Exception closing resultset", e)
        }
      }

      override protected def getNext(): InternalRow = {
        if (rs.next()) {
//          inputMetrics.incRecordsRead(1)
          var i = 0
          while (i < getters.length) {
            getters(i).apply(rs, mutableRow, i)
            if (rs.wasNull) mutableRow.setNullAt(i)
            i = i + 1
          }
          mutableRow
        } else {
          finished = true
          null.asInstanceOf[InternalRow]
        }
      }
    }
  }

  // A `JDBCValueGetter` is responsible for getting a value from `ResultSet` into a field
  // for `MutableRow`. The last argument `Int` means the index for the value to be set in
  // the row and also used for the value in `ResultSet`.
  type JDBCValueGetter = (ResultSet, InternalRow, Int) => Unit

  /**
   * Creates `JDBCValueGetter`s according to [[StructType]], which can set
   * each value from `ResultSet` to each field of [[InternalRow]] correctly.
   */
  def makeGetters(schema: StructType): Array[JDBCValueGetter] =
    schema.fields.map(sf => makeGetter(sf.dataType, sf.metadata))

  def makeGetter(dt: DataType, metadata: Metadata): JDBCValueGetter = dt match {
    case BooleanType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setBoolean(pos, rs.getBoolean(pos + 1))

    case DateType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        // DateTimeUtils.fromJavaDate does not handle null value, so we need to check it.
        val dateVal = rs.getDate(pos + 1)
        if (dateVal != null) {
          row.setInt(pos, DateTimeUtils.fromJavaDate(dateVal))
        } else {
          row.update(pos, null)
        }

    // When connecting with Oracle DB through JDBC, the precision and scale of BigDecimal
    // object returned by ResultSet.getBigDecimal is not correctly matched to the table
    // schema reported by ResultSetMetaData.getPrecision and ResultSetMetaData.getScale.
    // If inserting values like 19999 into a column with NUMBER(12, 2) type, you get through
    // a BigDecimal object with scale as 0. But the dataframe schema has correct type as
    // DecimalType(12, 2). Thus, after saving the dataframe into parquet file and then
    // retrieve it, you will get wrong result 199.99.
    // So it is needed to set precision and scale for Decimal based on JDBC metadata.
    case DecimalType.Fixed(p, s) =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val decimal =
          nullSafeConvert[java.math.BigDecimal](rs.getBigDecimal(pos + 1), d => Decimal(d, p, s))
        row.update(pos, decimal)

    case DoubleType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setDouble(pos, rs.getDouble(pos + 1))

    case FloatType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setFloat(pos, rs.getFloat(pos + 1))

    case IntegerType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setInt(pos, rs.getInt(pos + 1))

    case LongType if metadata.contains("binarylong") =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val bytes = rs.getBytes(pos + 1)
        var ans = 0L
        var j = 0
        while (j < bytes.length) {
          ans = 256 * ans + (255 & bytes(j))
          j = j + 1
        }
        row.setLong(pos, ans)

    case LongType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setLong(pos, rs.getLong(pos + 1))

    case ShortType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setShort(pos, rs.getShort(pos + 1))

    case ByteType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setByte(pos, rs.getByte(pos + 1))

    case StringType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        // TODO(davies): use getBytes for better performance, if the encoding is UTF-8
        row.update(pos, UTF8String.fromString(rs.getString(pos + 1)))

    case TimestampType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val t = rs.getTimestamp(pos + 1)
        if (t != null) {
          row.setLong(pos, DateTimeUtils.fromJavaTimestamp(t))
        } else {
          row.update(pos, null)
        }

    case BinaryType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.update(pos, rs.getBytes(pos + 1))

    case ArrayType(et, _) =>
      val elementConversion = et match {
        case TimestampType =>
          (array: Object) =>
            array.asInstanceOf[Array[java.sql.Timestamp]].map { timestamp =>
              nullSafeConvert(timestamp, DateTimeUtils.fromJavaTimestamp)
            }

        case StringType =>
          (array: Object) =>
            // some underling types are not String such as uuid, inet, cidr, etc.
            array.asInstanceOf[Array[java.lang.Object]]
              .map(obj => if (obj == null) null else UTF8String.fromString(obj.toString))

        case DateType =>
          (array: Object) =>
            array.asInstanceOf[Array[java.sql.Date]].map { date =>
              nullSafeConvert(date, DateTimeUtils.fromJavaDate)
            }

        case dt: DecimalType =>
          (array: Object) =>
            array.asInstanceOf[Array[java.math.BigDecimal]].map { decimal =>
              nullSafeConvert[java.math.BigDecimal](
                decimal, d => Decimal(d, dt.precision, dt.scale))
            }

        case LongType if metadata.contains("binarylong") =>
          throw new IllegalArgumentException(s"Unsupported array element " +
            s"type ${dt.catalogString} based on binary")

        case ArrayType(_, _) =>
          throw new IllegalArgumentException("Nested arrays unsupported")

        case _ => (array: Object) => array.asInstanceOf[Array[Any]]
      }

      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val array = nullSafeConvert[java.sql.Array](
          input = rs.getArray(pos + 1),
          array => new GenericArrayData(elementConversion.apply(array.getArray)))
        row.update(pos, array)

    case _ => throw new IllegalArgumentException(s"Unsupported type ${dt.catalogString}")
  }

  def nullSafeConvert[T](input: T, f: T => Any): Any = {
    if (input == null) {
      null
    } else {
      f(input)
    }
  }

  // A `JDBCValueSetter` is responsible for setting a value from `Row` into a field for
  // `PreparedStatement`. The last argument `Int` means the index for the value to be set
  // in the SQL statement and also used for the value in `Row`.
  type JDBCValueSetter = (PreparedStatement, InternalRow) => Unit

  def makeSetter(fieldIndex: Int, pos: Int, dataType: DataType): JDBCValueSetter = dataType match {
    case IntegerType =>
      (stmt: PreparedStatement, row: InternalRow) =>
        if (row.isNullAt(fieldIndex)) {
          stmt.setNull(pos, Types.INTEGER)
        } else {
          stmt.setInt(pos, row.getInt(fieldIndex))
        }
    case LongType =>
      (stmt: PreparedStatement, row: InternalRow) =>
        if (row.isNullAt(fieldIndex)) {
          stmt.setNull(pos, Types.BIGINT)
        } else {
          stmt.setLong(pos, row.getLong(fieldIndex))
        }

    case DoubleType =>
      (stmt: PreparedStatement, row: InternalRow) =>
        if (row.isNullAt(fieldIndex)) {
          stmt.setNull(pos, Types.DOUBLE)
        } else {
          stmt.setDouble(pos, row.getDouble(fieldIndex))
        }

    case FloatType =>
      (stmt: PreparedStatement, row: InternalRow) =>
        if (row.isNullAt(fieldIndex)) {
          stmt.setNull(pos, Types.FLOAT)
        } else {
          stmt.setFloat(pos, row.getFloat(fieldIndex))
        }

    case ShortType =>
      (stmt: PreparedStatement, row: InternalRow) =>
        if (row.isNullAt(fieldIndex)) {
          stmt.setNull(pos, Types.SMALLINT)
        } else {
          stmt.setInt(pos, row.getShort(fieldIndex))
        }

    case ByteType =>
      (stmt: PreparedStatement, row: InternalRow) =>
        if (row.isNullAt(fieldIndex)) {
          stmt.setNull(pos, Types.TINYINT)
        } else {
          stmt.setInt(pos, row.getByte(fieldIndex))
        }

    case BooleanType =>
      (stmt: PreparedStatement, row: InternalRow) =>
        if (row.isNullAt(fieldIndex)) {
          stmt.setNull(pos, Types.BOOLEAN)
        } else {
          stmt.setBoolean(pos, row.getBoolean(fieldIndex))
        }

    case StringType =>
      (stmt: PreparedStatement, row: InternalRow) =>
        if (row.isNullAt(fieldIndex)) {
          stmt.setNull(pos, Types.VARCHAR)
        } else {
          stmt.setString(pos, row.getString(fieldIndex))
        }

    case BinaryType =>
      (stmt: PreparedStatement, row: InternalRow) =>
        if (row.isNullAt(fieldIndex)) {
          stmt.setNull(pos, Types.BINARY)
        } else {
          stmt.setBytes(pos, row.getBinary(fieldIndex))
        }

    case TimestampType =>
      (stmt: PreparedStatement, row: InternalRow) =>
        if (row.isNullAt(fieldIndex)) {
          stmt.setNull(pos, Types.TIMESTAMP)
        } else {
          // see DeserializerBuildHelper.createDeserializerForSqlTimestamp()
          stmt.setTimestamp(pos, DateTimeUtils.toJavaTimestamp(row.getLong(fieldIndex)))
        }

    case DateType =>
      (stmt: PreparedStatement, row: InternalRow) =>
        if (row.isNullAt(fieldIndex)) {
          stmt.setNull(pos, Types.DATE)
        } else {
          // see DeserializerBuildHelper.createDeserializerForSqlDate()
          stmt.setDate(pos, DateTimeUtils.toJavaDate(row.getInt(fieldIndex)))
        }

    case DecimalType.Fixed(precision, scale) =>
      (stmt: PreparedStatement, row: InternalRow) =>
        if (row.isNullAt(fieldIndex)) {
          stmt.setNull(pos, Types.DECIMAL)
        } else {
          stmt.setBigDecimal(pos, row.getDecimal(fieldIndex, precision, scale).toJavaBigDecimal)
        }

    case _ =>
      (_: PreparedStatement, _: InternalRow) =>
        throw new IllegalArgumentException(
          s"Can't translate non-null value for field $pos")
  }
}
