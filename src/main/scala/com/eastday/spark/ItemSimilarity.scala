package com.eastday.spark


import java.util.Random

import com.eastday.domain.{ItemSimi, ItemPref}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.CompactBuffer
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math._
/**
 * Created by admin on 2018/5/7.
 */

class ItemSimilarity extends Serializable {


  def Similarity(user_rdd:RDD[(ItemPref)],stype:String):RDD[(ItemSimi)]={
    val similar_rdd =stype match{
      case "cooCurrence" => ItemSimilarity.cooCurrenceSimilarity(user_rdd)
      case "Pearson" =>{
        ItemSimilarity.pearsonSimilarity(user_rdd)
      }
      case "sparsePearson" => ItemSimilarity.sparsePearson(user_rdd)
      case "cos" => ItemSimilarity.cosSimilarity(user_rdd)
      case _ => throw new NoSuchMethodException("input method error")
    }
    similar_rdd
  }

}
object ItemSimilarity{

  /**
   * w(i,j) N(i) intersection(交集) N(j) /sqrt(N(i) * N(j))
   * @param user_rdd 用户评分矩阵
   * @return
   */
  def cooCurrenceSimilarity(user_rdd :RDD[(ItemPref)]) :RDD[(ItemSimi)]={
    /**
     * 1.数据准备
     * user_rdd1   (userid,itemid,pref)
     * user_rdd2   (userid,itemid)
     */
    val user_rdd1 :RDD[(String,String,Double)] = user_rdd.map(f => (f.userid,f.itemid,f.pref))
    val user_rdd2 :RDD[(String,String)] = user_rdd1.map(f => (f._1,f._2))
    //2.进行笛卡尔积
    val user_rdd3 :RDD[(String,(String,String))] =user_rdd2.join(user_rdd2)
    //((itemid1,itemid2)
    val user_rdd4 :RDD[((String,String),Long)] =user_rdd3.map(f => (f._2,1.toLong))
    //3(item1,item2,同现频次)
    val user_rdd5 :RDD[((String,String),Long)] = user_rdd4.reduceByKey(_ + _)
    //对角矩阵
    val user_rdd6 :RDD[((String,String),Long)]=user_rdd5.filter(f => f._1._1 ==f._1._2)
    //非对角矩阵
    val user_rdd7 :RDD[((String,String),Long)] =user_rdd5.filter(f => f._1._1 != f._1._2)

    val user_rdd8 :RDD[(String,((String,String,Long),Long))] = user_rdd7.map(f => (f._1._1 , (f._1._1,f._1._2,f._2)))
      .join(user_rdd6.map(f => (f._1._1,f._2)))
    val user_rdd9 :RDD[(String,((String,String,Long,Long),Long))]=user_rdd8.map(f => (f._2._1._2 ,(f._2._1._1,f._2._1._2,f._2._1._3,f._2._2)))
      .join(user_rdd6.map(f => (f._1._1,f._2)))
    val user_rdd10 = user_rdd9.map(f=>(f._2._1._1,f._2._1._2,f._2._1._3,f._2._1._4,f._2._2))
    val user_rdd11  =user_rdd10.map(f => (f._1,f._2,(f._3 / sqrt(f._4 * f._5))))
    user_rdd11.map(f => ItemSimi(f._1,f._2,f._3))
  }

  /**
   * person相关系数
   * @param user_rdd
   * @return
   */
  def pearsonSimilarity(user_rdd:RDD[(ItemPref)]):RDD[(ItemSimi)]={


//    val distinctUser =user_rdd.map(f=>(f.userid,0)).reduceByKey(_ + _ ).map(f=>f._1).zipWithIndex()
//    distinctUser.persist()
//
//    val distinctItem =user_rdd.map(f=>(f.itemid,0)).reduceByKey(_ + _).map(f=>f._1).zipWithIndex()
//    distinctItem.persist()
//
//    val dataWithUserIndex =user_rdd.map(f=>(f.userid,f)).join(distinctUser).map(f=>(f._2._1.itemid,(f._2._2,f._2._1.pref)))
//    val dataWithItemIndex =dataWithUserIndex.join(distinctItem).map(f=>(f._2._1._1,f._2._2,f._2._1._2))
//    val distinctUser =user_rdd.map(f=>(f.userid,0)).reduceByKey(_ + _ ).map(_._1)
//    val distinctItem =user_rdd.map(f=>(f.itemid,0)).reduceByKey(_ + _).map(_._1)
//    val joinUserAndItem =distinctUser.cartesian(distinctItem).coalesce(400).map(f=>((f._1,f._2),0))
//    val user_rdd1 =joinUserAndItem.leftOuterJoin(user_rdd.map(f=>((f.userid,f.itemid),f.pref)))
//      .map{
//        case ((userid,item),(_,option))=>{
//          val pref =option match {
//            case Some(pref) => pref
//            case None => 0.0
//          }
//          ItemPref(userid,item,pref)
//        }
//      }
//      user_rdd1.persist()
//    println(user_rdd1.count()+"  sum ")

//    user_rdd.filter(f => f.itemid.equals("73")||f.itemid.equals("9338") ).collect().foreach(println(_))
//    println("***********************************************************")
    //统计每个item的平均值
    val itemAvgRatingRDD: RDD[(String, Double)] = user_rdd.map(record => (record.itemid,record.pref))
      .groupByKey().map{
      case (itemID,iter) => {
        val (total, sum ) = iter.foldLeft((0,0.0))((a,b)=>{
          val v1 =a._1 + 1
          val v2 = a._1 + b
          (v1 ,v2 )
        })
        (itemID, 1.0 *sum /total)
      }
    }
//    itemAvgRatingRDD.filter(f => f._1.equals("73")||f._1.equals("9338") ).collect().foreach(println(_))
//    println("***********************************************************")
//        val itemAvgRatingRDD: RDD[(String, Double)] = user_rdd.map(record => (record.itemid,record.pref))
//          .reduceByKey(_ + _).map{
//          case (itemID,prefs) => {
//
//            (itemID, prefs /distinctUser)
//          }
//        }

      //去均值矩阵
    val removeItemAvgRatingRDD =user_rdd
      .map(record => (record.itemid,record))
      .join(itemAvgRatingRDD)
      .map{
        case (_,(record,avgPref)) =>{
            record.pref -= avgPref
          (record.userid,(record.itemid,record.pref))
        }
      }

//    val itemsPairRDD: RDD[((String, String), (Double, Double, Double))] = removeItemAvgRatingRDD .join(removeItemAvgRatingRDD)
//      .map{
//         case (_,(record1,record2))=>{
//           if(record1.itemid > record2.itemid) {
//              ((record1,record2),0)
//            }else{
//             ((record2,record1),0)
//           }
//         }
//      }.reduceByKey(_ + _)
//      .filter{
//        case ((ItemPref(_,item1,_),ItemPref(_,item2,_)),_)=>{
//          !item1.equals(item2)
//        }
//      }.map{
//        case ((ItemPref(_,item1,pref1),ItemPref(_,item2,pref2)),_)=>{
//          ((item1,item2),(pref1 * pref2 ,pref1 * pref1, pref2 * pref2))
//        }
//      }

    val groupbyUserIDRDD =removeItemAvgRatingRDD.
     // combineByKey()
      groupByKey()
//    for(i<- groupbyUserIDRDD.take(50)){
//      println(i)
//    }
      //println(groupbyUserIDRDD.count())
    val itemsPairRDD =groupbyUserIDRDD.flatMap{

      case (userID ,iter) =>{

        val list =iter.toList.sortBy(_._1.toLong)
        list.flatMap{
          case (itemid1,pref1) =>{
            list
              .filter(_._1.toLong >itemid1.toLong)
              .map{
                case (itemid2,pref2)=>{
                  val key = (itemid1,itemid2)
                  val prefMuti = pref1 * pref2
                  val pref1Square = pref1 * pref1
                  val pref2Square = pref2 * pref2
                  (key ,(prefMuti,pref1Square,pref2Square))
                }
            }
          }
        }
      }
    }
//    itemsPairRDD.filter(f => f._1._1.equals("73")&&f._1._2.equals("9338") ).collect().foreach(println(_))
//    println("***********************************************************")
    val resultRDD = itemsPairRDD.reduceByKey((v1,v2)=>(v1._1 + v2._1 , v1._2 + v2._2 , v1._3 + v2._3))
//    resultRDD.filter(f =>  f._1._1.equals("73")&&f._1._2.equals("9338") ).collect().foreach(println(_))
//    println("***********************************************************")
      val midRDD = resultRDD.map {
      case ((itemid1, itemid2), (prefMuti, pref1Square, pref2Square)) => {
       ItemSimi(itemid1, itemid2, prefMuti / (sqrt(pref1Square) * sqrt(pref2Square)))
      }
    }
//    val result1RDD =distinctItem.map(_.swap).join(resultRDD).map(f=>(f._2._2._1,(f._2._1,f._2._2._2))).join(distinctItem.map(_.swap))
//        .map(f=>ItemSimi(f._2._1._1,f._2._2,f._2._1._2))
    midRDD

  }


  def sparsePearson(user_rdd:RDD[(ItemPref)]):RDD[(ItemSimi)]={
    val distinctUser =user_rdd.map(f=>(f.userid,0)).reduceByKey(_ + _ ).map(f=>f._1).zipWithIndex()
    distinctUser.persist()

    val distinctItem =user_rdd.map(f=>(f.itemid,0)).reduceByKey(_ + _).map(f=>f._1).zipWithIndex()
    distinctItem.persist()
    val itemCount = distinctItem.count()

    val dataWithItemIndex =user_rdd.map(f=>(f.itemid,f)).join(distinctItem).map(_._2)
    val user_rdd1: RDD[Vector] =dataWithItemIndex.map(f=>(f._1.userid,(f._2,f._1.pref))).groupByKey()
      .join(distinctUser).sortBy(_._2._2)
      .map{
        case (_,(iter,userIndex))=>{
          val iter1 =iter.map(f=>(f._1.toInt,f._2)).toSeq
          Vectors.sparse(itemCount.toInt,iter1)
          }
        }

    val stat =Statistics.corr(user_rdd1,"pearson")
    println(stat)
    null
  }

  /**
   * 余弦公式相关系数
   * @param user_rdd
   * @return
   */
  def cosSimilarity(user_rdd:RDD[(ItemPref)]):RDD[(ItemSimi)]= {


   // 统计每个item的
    val itemAvgRatingRDD: RDD[(String, Double)] = user_rdd.map(record => (record.itemid, record.pref * record.pref))
      .reduceByKey(_ + _)

    //去均值矩阵
    val joinSumRatingRDD = user_rdd
      .map(record => (record.itemid, record))
      .join(itemAvgRatingRDD)
      .map {
        case (_, (record, sumPref)) => {
          (record.userid, (record.itemid, record.pref, sumPref))
        }
      }

    val random = new Random()
    val groupbyUserIDRDD = joinSumRatingRDD.groupByKey().map(f => (f._1,f._2.toIterator,random.nextInt(10)))



    val spiltGroupByUserIDRDD0 =groupbyUserIDRDD.filter(_._3 == 0).map(f=> (f._1,f._2)).repartition(5000)
    val spiltGroupByUserIDRDD1 =groupbyUserIDRDD.filter(_._3 == 1).map(f=> (f._1,f._2)).repartition(5000)
    val spiltGroupByUserIDRDD2 =groupbyUserIDRDD.filter(_._3 == 2).map(f=> (f._1,f._2)).repartition(5000)
    val spiltGroupByUserIDRDD3 =groupbyUserIDRDD.filter(_._3 == 3).map(f=> (f._1,f._2)).repartition(5000)
    val spiltGroupByUserIDRDD4 =groupbyUserIDRDD.filter(_._3 == 4).map(f=> (f._1,f._2)).repartition(5000)
    val spiltGroupByUserIDRDD5 =groupbyUserIDRDD.filter(_._3 == 5).map(f=> (f._1,f._2)).repartition(5000)
    val spiltGroupByUserIDRDD6 =groupbyUserIDRDD.filter(_._3 == 6).map(f=> (f._1,f._2)).repartition(5000)
    val spiltGroupByUserIDRDD7 =groupbyUserIDRDD.filter(_._3 == 7).map(f=> (f._1,f._2)).repartition(5000)
    val spiltGroupByUserIDRDD8 =groupbyUserIDRDD.filter(_._3 == 8).map(f=> (f._1,f._2)).repartition(5000)
    val spiltGroupByUserIDRDD9 =groupbyUserIDRDD.filter(_._3 == 9).map(f=> (f._1,f._2)).repartition(5000)

    def doubleChoose(rdd:RDD[(String, Iterator[(String, Double, Double)])]
                      ): RDD[((String, String), (Double, Double, Double))] ={
      rdd.flatMap {
        case (userID, iter) => {
          val list = iter.toList
          list.flatMap {
            case (itemid1, pref1,sum1) => {
              list
                .filter(_._1 > itemid1)
                .map {
                  case (itemid2, pref2,sum2) => {
                    val key = (itemid1, itemid2)
                    val prefMuti = pref1 * pref2
                    val pref1Square = sum1
                    val pref2Square = sum2
                    (key, (prefMuti, pref1Square, pref2Square))
                  }
                }
            }
          }
        }
      }
    }

    val doubleChooseRDD0=doubleChoose(spiltGroupByUserIDRDD0).reduceByKey((v1,v2)=>(v1._1 + v2._1 , v1._2 , v1._3),5000)
    val doubleChooseRDD1=doubleChoose(spiltGroupByUserIDRDD1).union(doubleChooseRDD0).reduceByKey((v1,v2)=>(v1._1 + v2._1 , v1._2 , v1._3),5000)
    val doubleChooseRDD2=doubleChoose(spiltGroupByUserIDRDD2).union(doubleChooseRDD1).reduceByKey((v1,v2)=>(v1._1 + v2._1 , v1._2 , v1._3),5000)
    val doubleChooseRDD3=doubleChoose(spiltGroupByUserIDRDD3).union(doubleChooseRDD2).reduceByKey((v1,v2)=>(v1._1 + v2._1 , v1._2 , v1._3),5000)
    val doubleChooseRDD4=doubleChoose(spiltGroupByUserIDRDD4).union(doubleChooseRDD3).reduceByKey((v1,v2)=>(v1._1 + v2._1 , v1._2 , v1._3),5000)
    val doubleChooseRDD5=doubleChoose(spiltGroupByUserIDRDD5).union(doubleChooseRDD4).reduceByKey((v1,v2)=>(v1._1 + v2._1 , v1._2 , v1._3),5000)
    val doubleChooseRDD6=doubleChoose(spiltGroupByUserIDRDD6).union(doubleChooseRDD5).reduceByKey((v1,v2)=>(v1._1 + v2._1 , v1._2 , v1._3),5000)
    val doubleChooseRDD7=doubleChoose(spiltGroupByUserIDRDD7).union(doubleChooseRDD6).reduceByKey((v1,v2)=>(v1._1 + v2._1 , v1._2 , v1._3),5000)
    val doubleChooseRDD8=doubleChoose(spiltGroupByUserIDRDD8).union(doubleChooseRDD7).reduceByKey((v1,v2)=>(v1._1 + v2._1 , v1._2 , v1._3),5000)
    val doubleChooseRDD9=doubleChoose(spiltGroupByUserIDRDD9).union(doubleChooseRDD8).reduceByKey((v1,v2)=>(v1._1 + v2._1 , v1._2 , v1._3),5000)



//
//    val itemsPairRDD =doubleChooseRDD0.union(doubleChooseRDD1)
//      .union(doubleChooseRDD2)
//      .union(doubleChooseRDD3)
//      .union(doubleChooseRDD4)
//      .union(doubleChooseRDD5)
//      .union(doubleChooseRDD6)
//      .union(doubleChooseRDD7)
//      .union(doubleChooseRDD8)
//      .union(doubleChooseRDD9)
    val resultRDD = doubleChooseRDD9.reduceByKey((v1,v2)=>(v1._1 + v2._1 , v1._2 , v1._3),2000)
//        resultRDD.filter(f =>  f._1._1.equals("千日红")&&f._1._2.equals("西湖") ).collect().foreach(println(_))
//        println("***********************************************************")
    val midRDD = resultRDD.map {
      case ((itemid1, itemid2), (prefMuti, pref1Square, pref2Square)) => {
        ItemSimi(itemid1, itemid2, prefMuti / (sqrt(pref1Square) * sqrt(pref2Square)))
      }
    }
    midRDD
  }

}