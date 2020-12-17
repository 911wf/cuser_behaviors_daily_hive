package com.xiaoe.cuser.UDAF

import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.xiaoe.cuser.Utils.CommonWork
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

// 历史上购买的产品
class boughtProductsFun extends UserDefinedAggregateFunction{
  // 输入参数类型
  override def inputSchema: StructType = {
    StructType(Array(StructField("created_at",DataTypes.StringType),
      StructField("order_state",DataTypes.IntegerType),
      StructField("settle_status",DataTypes.IntegerType),
      StructField("use_collection",DataTypes.IntegerType),
      StructField("goods_type",DataTypes.IntegerType),
      StructField("ship_state",DataTypes.IntegerType),
      StructField("order_id",DataTypes.StringType),
      StructField("price",DataTypes.IntegerType),
      StructField("product_id",DataTypes.StringType)
    ))
  }
  // 计算过程中临时数据类型
  override def bufferSchema: StructType = {
    StructType(Array(
      StructField("bought_products",DataTypes.createArrayType(DataTypes.StringType))
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
    val order_state = input.getAs[Int](1)
    val settle_status = input.getAs[Int](2)
    val use_collection = input.getAs[Int](3)
    val goods_type = input.getAs[Int](4)
    val ship_state = input.getAs[Int](5)
    val oid = input.getAs[String](6)
    val price = input.getAs[Int](7)
    val pid = input.getAs[String](8)
    if(order_state==1&&(settle_status==2||use_collection==0&&(goods_type!=1||ship_state<=0))){
      val seqToMap = new util.HashMap[String, String]()
      seqToMap.put("price",price.toString)
      seqToMap.put("tm",CommonWork.tranTimeToLongFun(created_at).toString)
      seqToMap.put("pid",pid)
      seqToMap.put("oid",oid)
      val bought_products = JSON.toJSONString(seqToMap, SerializerFeature.BeanToArray)
      buffer.update(0,buffer.getSeq(0).:+(bought_products))
    }
  }
  // 全局运算
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0,buffer1.getSeq(0).++:(buffer2.getSeq(0)))
  }

  override def evaluate(buffer: Row): Any = buffer.getSeq(0)
}
