package com.xiaoe.cuser.UDAF

import com.xiaoe.cuser.Utils.CommonWork
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

// 180天内下单行为的时间
class lastDaysOrderedAtFun extends UserDefinedAggregateFunction{
  // 输入参数类型
  override def inputSchema: StructType = {
    StructType(Array(StructField("created_at",DataTypes.StringType)))
  }
  // 计算过程中临时数据类型
  override def bufferSchema: StructType = {
    StructType(Array(
      StructField("latest_days_ordered_at",DataTypes.createArrayType(DataTypes.LongType))
    ))

  }
  // udaf返回值的类型
  override def dataType: DataType = DataTypes.createArrayType(DataTypes.LongType)
  // 判断每次传入的值类型是否一致
  override def deterministic: Boolean = true
  // 相当于每次获取值的时候 初始化操作
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0,ArrayBuffer[String]())
  }
  // 每次的局部运算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val created_at = CommonWork.tranTimeToLongFun(input.getAs[String](0))
    buffer.update(0,buffer.getSeq(0).:+(created_at))
  }
  // 全局运算
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0,buffer1.getSeq(0).++:(buffer2.getSeq(0)))
  }

  override def evaluate(buffer: Row): Any = buffer.getSeq(0)
}
