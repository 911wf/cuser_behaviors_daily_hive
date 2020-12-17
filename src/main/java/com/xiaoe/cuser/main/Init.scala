package com.xiaoe.cuser.main

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import com.xiaoe.cuser.UDAF.{boughtProductsFun, lastDaysOrderedAtFun, latestDaysPaidAtFun, latestValueFun, orderedProductsFun}
import com.xiaoe.cuser.Utils.CommonWork
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object Init {
  private val logger: Logger =
    LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("user_behaviors_sql")
      .getOrCreate()
    //    spark.conf.set("spark.debug.maxToStringFields", "100")

    import spark.implicits._

    //.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
    // .config("hadoop.home.dir", "/user/hive/warehouse")
    //      .enableHiveSupport()
    val sc = new SparkConf()
    var yesterdayTime = CommonWork.getYesterdayTime()
    var updateDate = sc.get("updateDate", yesterdayTime)
    //设置时间格式
    //    CommonWork.getCurrentTime()
    //
    //    logger.info(updateDate)
    //    logger.info(updateDate.split("=")(1))
    //    println("updateDate\t"+updateDate)
    //    println("updateDate.split('=')(1)\t"+updateDate.split("=")(1))
    val db_middle_m1_orders_dt = spark.read.option("header", value = true).csv("data/db_middle.m1_orders_dt.csv")
    val db_source_s_db_ex_business_t_users_df = spark.read.option("header", value = true).csv("data/db_source.s_db_ex_business_t_users.csv")
    val db_source_s_db_ex_business_t_purchase_d_df = spark.read.option("header", value = true).csv("data/db_source.s_db_ex_business_t_purchase_d.csv")
    val db_middle_m1_t_favorite_dt_df = spark.read.option("header", value = true).csv("data/db_middle.m1_t_favorite_dt.csv")
    val db_source_s_db_ex_punch_card_t_clock_actor_user_dt_df = spark.read.option("header", value = true).csv("data/db_source.s_db_ex_punch_card_t_clock_actor_user_dt.csv")
    val db_source_s_db_ex_examination_t_participate_exam_user_dt_df = spark.read.option("header", value = true).csv("data/db_source.s_db_ex_examination_t_participate_exam_user_dt.csv")
    val db_source_s_db_ex_business_t_exercises_answer_dt_df = spark.read.option("header", value = true).csv("data/db_source.s_db_ex_business_t_exercises_answer_dt.csv")
    val db_source_s_db_ex_business_t_community_user_dt_df = spark.read.option("header", value = true).csv("data/db_source.s_db_ex_business_t_community_user_dt.csv")
    val db_middle_m_c_report_log_d_df = spark.read.option("header", value = true).csv("data/db_middle.m_c_report_log_d.csv")
    val db_middle_m1_visit_log_h_df = spark.read.option("header", value = true).csv("data/db_middle.m1_visit_log_h.csv")

    val db_source_user_behaviors_t_purchase_df = spark.read.option("header", value = true).csv("data/db_source.user_behaviors_t_purchase.csv")

    db_middle_m1_orders_dt.createOrReplaceTempView("m1_orders_dt")
    db_source_s_db_ex_business_t_users_df.createOrReplaceTempView("s_db_ex_business_t_users")
    db_source_s_db_ex_business_t_purchase_d_df.createOrReplaceTempView("s_db_ex_business_t_purchase_d")
    db_middle_m1_t_favorite_dt_df.createOrReplaceTempView("m1_t_favorite_dt")
    db_source_s_db_ex_punch_card_t_clock_actor_user_dt_df.createOrReplaceTempView("s_db_ex_punch_card_t_clock_actor_user_dt")
    db_source_s_db_ex_examination_t_participate_exam_user_dt_df.createOrReplaceTempView("s_db_ex_examination_t_participate_exam_user_dt")
    db_source_s_db_ex_business_t_exercises_answer_dt_df.createOrReplaceTempView("s_db_ex_business_t_exercises_answer_dt")
    db_source_s_db_ex_business_t_community_user_dt_df.createOrReplaceTempView("s_db_ex_business_t_community_user_dt")
    db_middle_m_c_report_log_d_df.createOrReplaceTempView("m_c_report_log_d")
    db_middle_m1_visit_log_h_df.createOrReplaceTempView("m1_visit_log_h")

    db_source_user_behaviors_t_purchase_df.createOrReplaceTempView("user_behaviors_t_purchase")

    spark.udf.register("tranTimeToLongFun", CommonWork.tranTimeToLongFun)
    spark.udf.register("lastDaysOrderedAtFun", new lastDaysOrderedAtFun)
    spark.udf.register("orderedProductsFun", new orderedProductsFun)
    spark.udf.register("latestDaysPaidAtFun", new latestDaysPaidAtFun)
    spark.udf.register("boughtProductsFun", new boughtProductsFun)
    spark.udf.register("latestValueFun", new latestValueFun)
    spark.udf.register("standardizedDataFun", CommonWork.standardizedDataFun)
    //    var incr_query_t_users_sql =
    //      s"""
    //        |select `s_db_ex_business_t_users.app_id` app_id,`s_db_ex_business_t_users.user_id` user_id,`s_db_ex_business_t_users.phone` phone,`s_db_ex_business_t_users.phone_bak` collection_phone,`s_db_ex_business_t_users.wx_nickname` nick_name,`s_db_ex_business_t_users.wx_name` real_name,
    //        |`s_db_ex_business_t_users.created_at` user_created_at,`s_db_ex_business_t_users.extraInt` user_from,`s_db_ex_business_t_users.is_seal` user_identity,
    //        |`s_db_ex_business_t_users.wx_account` wx_id,`s_db_ex_business_t_users.wx_gender` gender,`s_db_ex_business_t_users.age` age,
    //        |year(`s_db_ex_business_t_users.birth`) birth_year,month(`s_db_ex_business_t_users.birth`) birth_month,day(`s_db_ex_business_t_users.birth`) birth_day,
    //        |`s_db_ex_business_t_users.wx_country` country,`s_db_ex_business_t_users.wx_province` province,`s_db_ex_business_t_users.wx_city` city,
    //        |`s_db_ex_business_t_users.wx_email` wx_email,`s_db_ex_business_t_users.industry` industry,`s_db_ex_business_t_users.company` company,`s_db_ex_business_t_users.job` job
    //        |from
    //        |s_db_ex_business_t_users
    //        |where `s_db_ex_business_t_users.updated_at` > "$updateDate" or `s_db_ex_business_t_users.created_at` > "$updateDate"
    //        |""".stripMargin

    //    var full_query_t_users_sql =
    //      s"""
    //         |select `s_db_ex_business_t_users.app_id` app_id,`s_db_ex_business_t_users.user_id` user_id,`s_db_ex_business_t_users.phone` phone,`s_db_ex_business_t_users.phone_bak` collection_phone,`s_db_ex_business_t_users.wx_nickname` nick_name,`s_db_ex_business_t_users.wx_name` real_name,
    //         |`s_db_ex_business_t_users.created_at` user_created_at,`s_db_ex_business_t_users.extraInt` user_from,`s_db_ex_business_t_users.is_seal` user_identity,
    //         |`s_db_ex_business_t_users.wx_account` wx_id,`s_db_ex_business_t_users.wx_gender` gender,`s_db_ex_business_t_users.age` age,
    //         |year(`s_db_ex_business_t_users.birth`) birth_year,month(`s_db_ex_business_t_users.birth`) birth_month,day(`s_db_ex_business_t_users.birth`) birth_day,
    //         |`s_db_ex_business_t_users.wx_country` country,`s_db_ex_business_t_users.wx_province` province,`s_db_ex_business_t_users.wx_city` city,
    //         |`s_db_ex_business_t_users.wx_email` wx_email,`s_db_ex_business_t_users.industry` industry,`s_db_ex_business_t_users.company` company,`s_db_ex_business_t_users.job` job
    //         |from
    //         |s_db_ex_business_t_users
    //         |""".stripMargin

    //    val t_user_df = spark.sql(full_query_t_users_sql).cache()
    //    t_user_df.createOrReplaceTempView("t_users")
    var full_query_t_purchase_sql =
    s"""
       |select
       |concat_ws('|',`s_db_ex_business_t_purchase_d.app_id`,`s_db_ex_business_t_purchase_d.user_id`) id,
       |latestValueFun(`s_db_ex_business_t_purchase_d.updated_at`,`s_db_ex_business_t_purchase_d.expire_at`) svip_expire_at
       |from s_db_ex_business_t_purchase_d
       |group by `s_db_ex_business_t_purchase_d.app_id`,`s_db_ex_business_t_purchase_d.user_id`
       |""".stripMargin
    var incr_query_t_purchase_sql =
      s"""
         |select
         |concat_ws('|',`s_db_ex_business_t_purchase_d.app_id`,`s_db_ex_business_t_purchase_d.user_id`) id,
         |latestValueFun(`s_db_ex_business_t_purchase_d.updated_at`,`s_db_ex_business_t_purchase_d.expire_at`) svip_expire_at
         |from s_db_ex_business_t_purchase_d
         |where `s_db_ex_business_t_purchase_d.updated_at` > "$updateDate" or `s_db_ex_business_t_purchase_d.created_at` > "$updateDate"
         |group by `s_db_ex_business_t_purchase_d.app_id`,`s_db_ex_business_t_purchase_d.user_id`
         |""".stripMargin

    val t_purchase_df = spark.sql(full_query_t_purchase_sql).cache()
    t_purchase_df.createOrReplaceTempView("t_purchase")
    var full_insert_t_purchase_sql =
      """
        |insert overwrite table user_behaviors_t_purchase
        | select id,svip_expire_at
        |from t_purchase
        |""".stripMargin
    spark.sql(full_insert_t_purchase_sql)

    var incr_query_t_orders_sql =
      """
        |select
        |concat_ws('|',`m1_orders_dt.app_id`,`m1_orders_dt.user_id`) id,
        |max(`m1_orders_dt.created_at`) latest_ordered_at,
        |lastDaysOrderedAtFun(`m1_orders_dt.created_at`) latest_days_ordered_ats_db_ex_business_t_users,
        |orderedProductsFun(`m1_orders_dt.created_at`,`m1_orders_dt.product_id`) ordered_products,
        |sum(`m1_orders_dt.price`) pay_amount,
        |count(`m1_orders_dt.price`) transaction_number,
        |latestDaysPaidAtFun(`m1_orders_dt.created_at`,`m1_orders_dt.order_state`,`m1_orders_dt.settle_status`,`m1_orders_dt.use_collection`,`m1_orders_dt.goods_type`,`m1_orders_dt.ship_state`) latest_days_paid_at,
        |max(unix_timestamp(`m1_orders_dt.created_at`))*1000 latest_paid_at,
        |boughtProductsFun(`m1_orders_dt.created_at`,`m1_orders_dt.order_state`,`m1_orders_dt.settle_status`,`m1_orders_dt.use_collection`,`m1_orders_dt.goods_type`,`m1_orders_dt.ship_state`,`m1_orders_dt.order_id`,`m1_orders_dt.price`,`m1_orders_dt.product_id`) bought_products
        |from
        |m1_orders_dt
        |where datediff(`m1_orders_dt.created_at`,current_timestamp()) < 180
        |group by `m1_orders_dt.app_id`,`m1_orders_dt.user_id`
        |""".stripMargin
    var full_query_t_orders_sql =
      """
        |select
        |concat_ws('|',`m1_orders_dt.app_id`,`m1_orders_dt.user_id`) id,
        |max(`m1_orders_dt.created_at`) latest_ordered_at,
        |lastDaysOrderedAtFun(`m1_orders_dt.created_at`) latest_days_ordered_ats_db_ex_business_t_users,
        |orderedProductsFun(`m1_orders_dt.created_at`,`m1_orders_dt.product_id`) ordered_products,
        |sum(`m1_orders_dt.price`) pay_amount,
        |count(`m1_orders_dt.price`) transaction_number,
        |latestDaysPaidAtFun(`m1_orders_dt.created_at`,`m1_orders_dt.order_state`,`m1_orders_dt.settle_status`,`m1_orders_dt.use_collection`,`m1_orders_dt.goods_type`,`m1_orders_dt.ship_state`) latest_days_paid_at,
        |max(unix_timestamp(`m1_orders_dt.created_at`))*1000 latest_paid_at,
        |boughtProductsFun(`m1_orders_dt.created_at`,`m1_orders_dt.order_state`,`m1_orders_dt.settle_status`,`m1_orders_dt.use_collection`,`m1_orders_dt.goods_type`,`m1_orders_dt.ship_state`,`m1_orders_dt.order_id`,`m1_orders_dt.price`,`m1_orders_dt.product_id`) bought_products
        |from
        |m1_orders_dt
        |group by `m1_orders_dt.app_id`,`m1_orders_dt.user_id`
        |""".stripMargin
    val t_order_df = spark.sql(full_query_t_orders_sql).cache()
    t_order_df.createOrReplaceTempView("t_orders")
    var full_insert_t_order_sql =
      """
        |insert overwrite table user_behaviors_t_orders
        | select id,latest_ordered_at,latest_days_ordered_ats_db_ex_business_t_users,ordered_products,pay_amount,transaction_number,latest_days_paid_at,latest_paid_at,bought_products
        |from t_orders
        |""".stripMargin
    spark.sql(full_insert_t_order_sql)



    var full_query_report_log_sql =
      """
        |select
        |concat_ws('|',`m_c_report_log_d.app_id`,`m_c_report_log_d.user_id`) id,
        |lastDaysOrderedAtFun(`m_c_report_log_d.visited_at`) latest_days_visited_at,
        |max(unix_timestamp(`m_c_report_log_d.visited_at`))*1000 latest_visited_at
        |from m_c_report_log_d
        |group by `m_c_report_log_d.app_id`,`m_c_report_log_d.user_id`
        |""".stripMargin
    var incr_query_report_log_sql =
      """
        |select
        |concat_ws('|',`m_c_report_log_d.app_id`,`m_c_report_log_d.user_id`) id,
        |lastDaysOrderedAtFun(`m_c_report_log_d.visited_at`) latest_days_visited_at,
        |max(unix_timestamp(`m_c_report_log_d.visited_at`))*1000 latest_visited_at
        |from m_c_report_log_d
        |where `m_c_report_log_d.visited_at` > "$updateDate"
        |group by `m_c_report_log_d.app_id`,`m_c_report_log_d.user_id`
        |""".stripMargin

    val report_log_df = spark.sql(full_query_report_log_sql).cache()
    report_log_df.createOrReplaceTempView("report_log")
    var full_insert_report_log_sql =
      """
        |insert overwrite table user_behaviors_report_log
        | select id,latest_days_visited_at,latest_visited_at
        |from report_log
        |""".stripMargin
    spark.sql(full_insert_report_log_sql)




    var full_query_visit_log_sql =
      """
        |select
        |concat_ws('|',`m1_visit_log_h.app_id`,`m1_visit_log_h.user_id`) id,
        |orderedProductsFun(`m1_visit_log_h.visited_at`,`m1_visit_log_h.product_id`) visited_products
        |from m1_visit_log_h
        |group by `m1_visit_log_h.app_id`,`m1_visit_log_h.user_id`
        |""".stripMargin
    var incr_query_visit_log_sql =
      """
        |select
        |concat_ws('|',`m1_visit_log_h.app_id`,`m1_visit_log_h.user_id`) id,
        |orderedProductsFun(`m1_visit_log_h.visited_at`,`m1_visit_log_h.product_id`) visited_products
        |from m1_visit_log_h
        |where `m1_visit_log_h.visited_at` > "$updateDate"
        |group by `m1_visit_log_h.app_id`,`m1_visit_log_h.user_id`
        |""".stripMargin
    val visit_log_df = spark.sql(full_query_visit_log_sql).cache()
    visit_log_df.createOrReplaceTempView("visit_log")
    var full_insert_visit_log_sql =
      """
        |insert overwrite table user_behaviors_visit_log
        | select id,visited_products
        |from visit_log
        |""".stripMargin
    spark.sql(full_insert_visit_log_sql)



    var full_query_t_favorite_sql =
      """
        |select
        |concat_ws('|',`m1_t_favorite_dt.app_id`,`m1_t_favorite_dt.user_id`) id,
        |orderedProductsFun(`m1_t_favorite_dt.created_at`,`m1_t_favorite_dt.content_id`) favorites_products
        |from m1_t_favorite_dt
        |group by `m1_t_favorite_dt.app_id`,`m1_t_favorite_dt.user_id`
        |""".stripMargin
    var incr_query_t_favorite_sql =
      """
        |select
        |concat_ws('|',`m1_t_favorite_dt.app_id`,`m1_t_favorite_dt.user_id`) id,
        |orderedProductsFun(`m1_t_favorite_dt.created_at`,`m1_t_favorite_dt.content_id`) favorites_products
        |from m1_t_favorite_dt
        |where `m1_t_favorite_dt.updated_at` > "$updateDate" or `m1_t_favorite_dt.created_at` > "$updateDate"
        |group by `m1_t_favorite_dt.app_id`,`m1_t_favorite_dt.user_id`
        |""".stripMargin
    val t_favorite_df = spark.sql(full_query_t_favorite_sql).cache()
    t_favorite_df.createOrReplaceTempView("t_favorite")
    var full_insert_t_favorite_sql =
      """
        |insert overwrite table user_behaviors_t_favorite
        | select id,favorites_products
        |from t_favorite
        |""".stripMargin
    spark.sql(full_insert_t_favorite_sql)




    var full_incr_t_clock_sql =
      """
        |select
        |concat_ws('|',`s_db_ex_punch_card_t_clock_actor_user_dt.app_id`,`s_db_ex_punch_card_t_clock_actor_user_dt.user_id`) id,
        |orderedProductsFun(`s_db_ex_punch_card_t_clock_actor_user_dt.created_at`,`s_db_ex_punch_card_t_clock_actor_user_dt.activity_id`) clock_records
        |from s_db_ex_punch_card_t_clock_actor_user_dt
        |group by `s_db_ex_punch_card_t_clock_actor_user_dt.app_id`,`s_db_ex_punch_card_t_clock_actor_user_dt.user_id`
        |""".stripMargin
    var incr_incr_t_clock_sql =
      """
        |select
        |concat_ws('|',`s_db_ex_punch_card_t_clock_actor_user_dt.app_id`,`s_db_ex_punch_card_t_clock_actor_user_dt.user_id`) id,
        |orderedProductsFun(`s_db_ex_punch_card_t_clock_actor_user_dt.created_at`,`s_db_ex_punch_card_t_clock_actor_user_dt.activity_id`) clock_records
        |from s_db_ex_punch_card_t_clock_actor_user_dt
        |where `s_db_ex_punch_card_t_clock_actor_user_dt.updated_at` > "$updateDate" or `s_db_ex_punch_card_t_clock_actor_user_dt.created_at` > "$updateDate"
        |group by `s_db_ex_punch_card_t_clock_actor_user_dt.app_id`,`s_db_ex_punch_card_t_clock_actor_user_dt.user_id`
        |""".stripMargin
    val t_clock_df = spark.sql(full_incr_t_clock_sql).cache()
    t_clock_df.createOrReplaceTempView("t_clock")
    var full_insert_t_clock_sql =
      """
        |insert overwrite table user_behaviors_t_clock
        | select id,clock_records
        |from t_clock
        |""".stripMargin
    spark.sql(full_insert_t_clock_sql)



    var full_query_t_participate_exam_user_sql =
      """
        |select
        |concat_ws('|',`s_db_ex_examination_t_participate_exam_user_dt.app_id`,`s_db_ex_examination_t_participate_exam_user_dt.user_id`) id,
        |orderedProductsFun(`s_db_ex_examination_t_participate_exam_user_dt.created_at`,`s_db_ex_examination_t_participate_exam_user_dt.exam_id`) examination_records
        |from s_db_ex_examination_t_participate_exam_user_dt
        |group by `s_db_ex_examination_t_participate_exam_user_dt.app_id`,`s_db_ex_examination_t_participate_exam_user_dt.user_id`
        |""".stripMargin
    var incr_query_t_participate_exam_user_sql =
      """
        |select
        |concat_ws('|',`s_db_ex_examination_t_participate_exam_user_dt.app_id`,`s_db_ex_examination_t_participate_exam_user_dt.user_id`) id,
        |orderedProductsFun(`s_db_ex_examination_t_participate_exam_user_dt.created_at`,`s_db_ex_examination_t_participate_exam_user_dt.exam_id`) examination_records
        |from s_db_ex_examination_t_participate_exam_user_dt
        |where `s_db_ex_examination_t_participate_exam_user_dt.updated_at` > "$updateDate" or `s_db_ex_examination_t_participate_exam_user_dt.created_at` > "$updateDate"
        |group by `s_db_ex_examination_t_participate_exam_user_dt.app_id`,`s_db_ex_examination_t_participate_exam_user_dt.user_id`
        |""".stripMargin
    val t_participate_exam_user_df = spark.sql(full_query_t_participate_exam_user_sql).cache()
    t_participate_exam_user_df.createOrReplaceTempView("t_participate_exam_user")
    var full_insert_t_participate_exam_user_sql =
      """
        |insert overwrite table user_behaviors_t_participate_exam_user
        | select id,examination_records
        |from t_participate_exam_user
        |""".stripMargin
    spark.sql(full_insert_t_participate_exam_user_sql)



    var full_query_t_exercises_answer_sql =
      """
        |select
        |concat_ws('|',`s_db_ex_business_t_exercises_answer_dt.app_id`,`s_db_ex_business_t_exercises_answer_dt.answer_user_id`) id,
        |orderedProductsFun(`s_db_ex_business_t_exercises_answer_dt.created_at`,`s_db_ex_business_t_exercises_answer_dt.exercise_id`) exercise_id
        |from s_db_ex_business_t_exercises_answer_dt
        |group by `s_db_ex_business_t_exercises_answer_dt.app_id`,`s_db_ex_business_t_exercises_answer_dt.answer_user_id`
        |""".stripMargin
    var incr_query_t_exercises_answer_sql =
      """
        |select
        |concat_ws('|',`s_db_ex_business_t_exercises_answer_dt.app_id`,`s_db_ex_business_t_exercises_answer_dt.answer_user_id`) id,
        |orderedProductsFun(`s_db_ex_business_t_exercises_answer_dt.created_at`,`s_db_ex_business_t_exercises_answer_dt.exercise_id`) exercise_id
        |from s_db_ex_business_t_exercises_answer_dt
        |where `s_db_ex_business_t_exercises_answer_dt.updated_at` > "$updateDate" or `s_db_ex_business_t_exercises_answer_dt.created_at` > "$updateDate"
        |group by `s_db_ex_business_t_exercises_answer_dt.app_id`,`s_db_ex_business_t_exercises_answer_dt.answer_user_id`
        |""".stripMargin
    val t_exercises_answer_df = spark.sql(full_query_t_exercises_answer_sql).cache()
    t_exercises_answer_df.createOrReplaceTempView("t_exercises_answer")
    var full_insert_t_exercises_answer_sql =
      """
        |insert overwrite table user_behaviors_t_exercises_answer
        | select id,exercise_id
        |from t_exercises_answer
        |""".stripMargin
    spark.sql(full_insert_t_exercises_answer_sql)




    var full_query_t_community_user_sql =
      """
        |select
        |concat_ws('|',`s_db_ex_business_t_community_user_dt.app_id`,`s_db_ex_business_t_community_user_dt.user_id`) id,
        |orderedProductsFun(`s_db_ex_business_t_community_user_dt.created_at`,`s_db_ex_business_t_community_user_dt.community_id`) community_records
        |from s_db_ex_business_t_community_user_dt
        |group by `s_db_ex_business_t_community_user_dt.app_id`,`s_db_ex_business_t_community_user_dt.user_id`
        |""".stripMargin
    var incr_query_t_community_user_sql =
      """
        |select
        |concat_ws('|',`s_db_ex_business_t_community_user_dt.app_id`,`s_db_ex_business_t_community_user_dt.user_id`) id,
        |orderedProductsFun(`s_db_ex_business_t_community_user_dt.created_at`,`s_db_ex_business_t_community_user_dt.community_id`) community_records
        |from s_db_ex_business_t_community_user_dt
        |where `s_db_ex_business_t_community_user_dt.updated_at` > "$updateDate" or `s_db_ex_business_t_community_user_dt.created_at` > "$updateDate"
        |group by `s_db_ex_business_t_community_user_dt.app_id`,`s_db_ex_business_t_community_user_dt.user_id`
        |""".stripMargin
    val t_community_user_df = spark.sql(full_query_t_community_user_sql).cache()
    t_community_user_df.createOrReplaceTempView("t_community_user")
    var full_insert_t_community_user_sql =
      """
        |insert overwrite table user_behaviors_t_community_user
        | select id,community_records
        |from t_community_user
        |""".stripMargin
    spark.sql(full_insert_t_community_user_sql)

    spark.stop()
  }

}
