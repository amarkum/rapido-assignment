package com.rapido.bike.sparkudaf

import java.time.format.DateTimeFormatter
import java.time.temporal.IsoFields
import java.time.{ZoneId, ZonedDateTime}

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{StructField, _}

class DailyAvg() extends UserDefinedAggregateFunction {

  def inputSchema: StructType = StructType(Array(StructField("ts", TimestampType)))

  def bufferSchema = StructType(Array(
    StructField("totalBookings", IntegerType),
    StructField("distinctDayhCount", IntegerType),
    StructField("previousYear", IntegerType),
    StructField("currentYear", IntegerType),
    StructField("previousMonth", IntegerType),
    StructField("currentMonth", IntegerType),
    StructField("previousWeek", IntegerType),
    StructField("CurrentWeek", IntegerType),
    StructField("PreviousDay", IntegerType),
    StructField("CurrentDay", IntegerType)
  ))

  def dataType: DataType = DoubleType

  def deterministic = true

  def initialize(buffer: MutableAggregationBuffer) = {
    buffer(0) = 0
    buffer(1) = 1

    // Year Buffer
    buffer(2) = 0
    buffer(3) = 0

    // Month Buffer
    buffer(4) = 0
    buffer(5) = 0

    // Weekly Buffer
    buffer(6) = 0
    buffer(7) = 0

    // Daily Buffer
    buffer(8) = 0
    buffer(9) = 0

  }

  def update(buffer: MutableAggregationBuffer, input: Row) = {
    val dateString = input(0).toString()
    val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S")
    val zdt = ZonedDateTime.parse(dateString, dtf.withZone(ZoneId.systemDefault))

    if (buffer.getInt(7) == 0) {
      buffer(3) = zdt.getYear()
      buffer(5) = zdt.getMonthValue()
      buffer(7) = zdt.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)
      buffer(9) = zdt.getDayOfWeek.getValue
    }
    buffer(2) = buffer(3)
    buffer(3) = zdt.getYear()

    buffer(4) = buffer(5)
    buffer(5) = zdt.getMonthValue()

    buffer(6) = buffer(7)
    buffer(7) = zdt.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)

    buffer(8) = buffer(9)
    buffer(9) = zdt.getDayOfWeek.getValue

    if ( buffer.getInt(9) != buffer.getInt(8) || buffer.getInt(7) != buffer.getInt(6)
      || buffer.getInt(5) != buffer.getInt(4) || buffer.getInt(3) != buffer.getInt(2)) {
      buffer(1) = buffer.getInt(1) + 1
    }
    buffer(0) = buffer.getInt(0) + 1
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    buffer1(0) = buffer2.getInt(0)
    buffer1(1) = buffer2.getInt(1)
  }

  def evaluate(buffer: Row) = {
    buffer.getInt(0).asInstanceOf[Double] / buffer.getInt(1)
  }

}