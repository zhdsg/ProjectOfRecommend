package com.eastday.spark

import com.eastday.conf.ConfigurationManager
import com.eastday.constract.Constract
import com.eastday.domain.{ItemPref, ItemSimi, UserRecomm}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD

/**
 * Created by admin on 2018/5/8.
 */
object RecommendItem {

  def recommend(userRDD:RDD[(ItemPref)],similarRDD:RDD[(ItemSimi)],num :Int,topN:Int ): RDD[(UserRecomm)] ={


    //(item,list((item1,pref1),(item2,pref2),...))
    val selectRDDforSimi: RDD[(String, Iterator[(String, Double)])]  =similarRDD.flatMap{
      case (ItemSimi(item1,item2,similar))=>{
        (item1,(item2,similar)) :: (item2,(item1,similar)) :: Nil
      }
    }.groupByKey().map{
      case(item,iter) =>{
        val list =iter.toList.sortBy(_._2)
        .takeRight(num)
        (item,iter.toIterator)
      }
    }

//    for(i <- selectRDDforSimi.take(10)){
//      println(i)
//    }

    val userRatingOfNewItem: RDD[((String, String), Double)] = userRDD.map(f=> (f.itemid,(f.userid,f.pref)))
      .join(selectRDDforSimi)
      .flatMap{
        case(item,((userid,pref),iter)) =>{
          iter.map{
            case (other_item,similar)=>{
              ((userid,other_item),similar*pref)
            }
          }
        }
      }
    val sumRatingRDD: RDD[((String, String), Double)] =userRatingOfNewItem.reduceByKey(_ + _)
    for(i <- sumRatingRDD.take(10)){
      println(i)
    }
    //过滤已有物品
    val filterUnRecommItem: RDD[(String, (String, Double))] = sumRatingRDD.leftOuterJoin(userRDD.map(f => ((f.userid,f.itemid),1)))
      .filter(f => f._2._2.isEmpty).map(f=> (f._1._1,(f._1._2,f._2._1)))

  //  val filterItemForValue: RDD[(String, (String, Double))] =filterUnRecommItem.filter(_._2._2 > value)
//    for(i <- filterItemForValue.take(100)){
//        println(i)
//      }
        val resultRDD: RDD[UserRecomm] =filterUnRecommItem.groupByKey().flatMap{
          case (userid,iter) =>{
            val list = iter.toList.sortBy(_._2)
              .takeRight(topN)

            list.map{
              case (item,recommPref)=>UserRecomm(userid,item,recommPref)
            }
          }
    }
    resultRDD
  }

  def main(args: Array[String]) {
    if(args.length !=2 ){
      println("usege   5000 20 ")
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

    val num = args(0)
    val topN =args(1)
    val user_rdd =DataSource.getDataSource(sc)
    val rdds =DataSource.getDataSourceOfItemSimi(sc)
    val fs =FileSystem.get(new Configuration())

    val userRecomm: RDD[UserRecomm] =RecommendItem.recommend(user_rdd,rdds,num.toInt,topN.toInt)

    Result4Save.save4UserRecommTable(fs,userRecomm)
  }
}
