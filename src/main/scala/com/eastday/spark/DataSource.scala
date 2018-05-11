package com.eastday.spark

import com.eastday.domain.{ItemSimi, ItemPref}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.Try

/**
 * Created by admin on 2018/5/7.
 */
object DataSource {


  def getDataSource(sc:SparkContext):RDD[(ItemPref)]={
   // val path = "hdfs://Ucluster/user/hive/warehouse/minieastdaywap.db/t_20180502_tag_app_2";
//    val path = "hdfs://Ucluster/user/hive/warehouse/minieastdaywap.db/tmp_zh_20180502_tag_app_2"
    val path ="hdfs://Ucluster/user/hive/warehouse/minieastdaywap.db/tmp_zh_20180502_tag_app_2"
    val data: RDD[ItemPref] = sc.textFile(path).map(f => {
      val splits =f.split("\u0001")
      Try(ItemPref(splits(0),splits(1),splits(2).trim.toDouble))
    }).filter(_.isSuccess).map(_.get)
    data
  }
  def getDataSourceOfItemSimi(sc:SparkContext):RDD[(ItemSimi)]={
    // val path = "hdfs://Ucluster/user/hive/warehouse/minieastdaywap.db/t_20180502_tag_app_2";
    //    val path = "hdfs://Ucluster/user/hive/warehouse/minieastdaywap.db/tmp_zh_20180502_tag_app_2"
    val path ="hdfs://Ucluster/user/hive/warehouse/minieastdaywap.db/tmp_result_simliar"
    val data: RDD[ItemSimi] = sc.textFile(path).map(f => {
      val splits =f.split("\t")
      Try(ItemSimi(splits(0),splits(1),splits(2).trim.toDouble))
    }).filter(_.isSuccess).map(_.get)
    data
  }
}
