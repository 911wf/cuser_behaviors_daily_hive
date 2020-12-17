package com.xiaoe.cuser.main

import com.xiaoe.cuser.UDAF._
import com.xiaoe.cuser.Utils.CommonWork
import org.apache.spark.sql.SparkSession

/*

spark-submit  --class com.xiaoe.cuser.main.MyTestReadHive  --master yarn --deploy-mode client --driver-memory 2g  --executor-memory 1g --executor-cores 1   cuser_behaviors_daily_hive-1.0-SNAPSHOT.jar

 */
object  MyTestReadHive {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("user_behaviors_sql")
      .config("spark.sql.warehouse.dir", "/usr/hive/warehouse")
      .config("hive.exec.orc.split.strategy", "BI")
      .enableHiveSupport()
      .getOrCreate()

    spark.udf.register("tranTimeToLongFun", CommonWork.tranTimeToLongFun)
    spark.udf.register("lastDaysOrderedAtFun", new lastDaysOrderedAtFun)
    spark.udf.register("orderedProductsFun", new orderedProductsFun)
    spark.udf.register("latestDaysPaidAtFun", new latestDaysPaidAtFun)
    spark.udf.register("boughtProductsFun", new boughtProductsFun)
    spark.udf.register("latestValueFun", new latestValueFun)
    spark.udf.register("standardizedDataFun", CommonWork.standardizedDataFun)



    var full_query_t_purchase_sql =
      s"""
         |select
         |concat_ws('|',`app_id`,`user_id`) id,
         |latestValueFun(`updated_at`,`expire_at`) svip_expire_at
         |from db_source.s_db_ex_business_t_purchase_d
         |where statdate = '20201216'
         |group by `app_id`,`user_id`
         |""".stripMargin


    print("full_query_t_purchase_sql: " + full_query_t_purchase_sql)

    val t_purchase_df = spark.sql(full_query_t_purchase_sql).cache()
    t_purchase_df.createOrReplaceTempView("t_purchase")

    t_purchase_df.show(10)


    var full_insert_t_purchase_sql =
      """
        |insert overwrite table db_result.user_behaviors_t_purchase
        |partition(statdate='20201216')
        |select id,svip_expire_at
        |from t_purchase
        |""".stripMargin
    spark.sql(full_insert_t_purchase_sql)


  }
}
