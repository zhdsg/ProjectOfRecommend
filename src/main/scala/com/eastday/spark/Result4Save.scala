package com.eastday.spark

import com.eastday.domain.{UserRecomm, ItemSimi}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD

/**
 * Created by admin on 2018/5/11.
 */
object Result4Save {
  def  save4ItemSimilarTable(fs:FileSystem , rdds :RDD[(ItemSimi)])={
    val path ="hdfs://Ucluster/user/hive/warehouse/minieastdaywap.db/tmp_result_simliar"
    if(fs.exists(new Path(path))){
      fs.delete(new Path(path))
    }
    rdds.map(f=>
      (s"${f.itemid1}\t${f.itemid2}\t${f.similar}")
    ).saveAsTextFile(path)
    println("records has been save at table tmp_result_simliar ...")
  }
  def  save4UserRecommTable(fs:FileSystem , userRecomm :RDD[(UserRecomm)])={
    val path1 ="hdfs://Ucluster/user/hive/warehouse/minieastdaywap.db/tmp_recommend"
    if(fs.exists(new Path(path1))){
      fs.delete(new Path(path1))
    }
    userRecomm.map(f=>
      (s"${f.userid}\t${f.itemid}\t${f.pref}")
    ).saveAsTextFile(path1)
    println("records has been save at table tmp_recommend ...")
  }
}
