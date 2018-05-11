package com.eastday.spark



import com.eastday.conf.ConfigurationManager
import com.eastday.constract.Constract
import com.eastday.domain.{UserRecomm, ItemSimi}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by admin on 2018/5/7.
 */
object ItemCF {
  def main(args: Array[String]) {

    if(args.length !=1 ){
       println("usege  'cos'  ")
      System.exit(-1)
    }
    val conf: SparkConf = new SparkConf()
      .setAppName(Constract.SPARK_APP_NAME)
      .set(Constract.SPARK_SHUFFLE_CONSOLIDATEFILES
        , ConfigurationManager.getString(Constract.SPARK_SHUFFLE_CONSOLIDATEFILES))
      .set(Constract.SPARK_SHUFFLE_FILE_BUFFER
        , ConfigurationManager.getString(Constract.SPARK_SHUFFLE_FILE_BUFFER))
      .set(Constract.SPARK_REDUCER_MAXSIZEINFLIGHT
        , ConfigurationManager.getString(Constract.SPARK_REDUCER_MAXSIZEINFLIGHT))
      .set(Constract.SPARK_SHUFFLE_IO_MAXRETRIES
        , ConfigurationManager.getString(Constract.SPARK_SHUFFLE_IO_MAXRETRIES))
//      .set(Constract.SPARK_DEFAULT_PARALLELISM
//        , ConfigurationManager.getString(Constract.SPARK_DEFAULT_PARALLELISM))


    val sc = new SparkContext(conf)
    val method =args(0)


    val similar  = new ItemSimilarity
    val user_rdd =DataSource.getDataSource(sc)
   // user_rdd=user_rdd.persist()
    val rdds: RDD[ItemSimi] = similar.Similarity(user_rdd,method)
    //rdds.persist()

    val fs =FileSystem.get(new Configuration())
    Result4Save.save4ItemSimilarTable(fs,rdds)





//    for(rdd <- rdds.take(100)){
//      println(s"${rdd.itemid1}    ${rdd.itemid2}   ${rdd.similar}")
//    }
//    println(rdds.count())
  }
}
