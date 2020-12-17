package com.xiaoe.cuser.UDAF

import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.xiaoe.cuser.Utils.CommonWork
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

import scala.collection.mutable.ArrayBuffer
//最近一次用户指定属性
class latestValueFun extends UserDefinedAggregateFunction{
  // 输入参数类型
  override def inputSchema: StructType = {
    StructType(Array(StructField("updated_at",DataTypes.StringType),
      StructField("value",DataTypes.StringType)
    ))
  }
  // 计算过程中临时数据类型
  override def bufferSchema: StructType = {
    StructType(Array(
      StructField("updated_at",DataTypes.StringType),
      StructField("latestValue",DataTypes.StringType)
    ))

  }
  // udaf返回值的类型
  override def dataType: DataType = DataTypes.StringType
  // 判断每次传入的值类型是否一致
  override def deterministic: Boolean = true
  // 相当于每次获取值的时候 初始化操作
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0,"0000-00-00 00:00:00")
    buffer.update(1,"")
  }
  // 每次的局部运算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val created_at = input.getAs[String](0)
    val value = input.getAs[String](1)
    val newTime = CommonWork.tranTimeToLongFun(created_at)
    val oldTime = CommonWork.tranTimeToLongFun(buffer.getString(0))
    if(newTime > oldTime){
      buffer.update(0,created_at)
      buffer.update(1,value)
    }
  }
  // 全局运算
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val buffer1Time = CommonWork.tranTimeToLongFun(buffer1.getString(0))
    val buffer2Time = CommonWork.tranTimeToLongFun(buffer2.getString(0))
    if(buffer2Time > buffer1Time){
      buffer1.update(0,buffer1.getString(0))
      buffer1.update(1,buffer2.getString(1))
    }
  }

  override def evaluate(buffer: Row): Any = buffer.getString(1)
}
