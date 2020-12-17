package com.xiaoe.cuser.UDAF

import java.util

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.xiaoe.cuser.Utils.CommonWork
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

//历史上订购的产品
class orderedProductsFun  extends UserDefinedAggregateFunction{
  // 输入参数类型
  override def inputSchema: StructType = {
    StructType(Array(StructField("created_at",DataTypes.StringType),
      StructField("product_id",DataTypes.StringType)))
  }
  // 计算过程中临时数据类型
  override def bufferSchema: StructType = {
    StructType(Array(
      StructField("ordered_products",DataTypes.createArrayType(DataTypes.StringType))
    ))

  }
  // udaf返回值的类型
  override def dataType: DataType = DataTypes.createArrayType(DataTypes.StringType)
  // 判断每次传入的值类型是否一致
  override def deterministic: Boolean = true
  // 相当于每次获取值的时候 初始化操作
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0,ArrayBuffer[String]())
  }
  // 每次的局部运算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val created_at = input.getAs[String](0)
    val product_id = input.getAs[String](1)
    val seqToMap = new util.HashMap[String, String]()
    seqToMap.put("tm",CommonWork.tranTimeToLongFun(created_at).toString)
    seqToMap.put("pid",product_id)
    val ordered_products = JSON.toJSONString(seqToMap, SerializerFeature.BeanToArray)
    buffer.update(0,buffer.getSeq(0).:+(ordered_products))
  }
  // 全局运算
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0,buffer1.getSeq(0).++:(buffer2.getSeq(0)))
  }

  override def evaluate(buffer: Row): Any = buffer.getSeq(0)
}
