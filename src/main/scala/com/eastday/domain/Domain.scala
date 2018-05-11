package com.eastday.domain

/**
 * Created by admin on 2018/5/7.
 */
/**
 *
 * @param userid
 * @param itemid
 * @param pref
 */
case class ItemPref( userid:String,itemid:String,var pref:Double) extends Serializable

/**
 *
 * @param userid
 * @param itemid
 * @param pref
 */
case class UserRecomm(userid:String,itemid :String, var pref:Double) extends Serializable

/**
 *
 * @param itemid1
 * @param itemid2
 * @param similar
 */
case class ItemSimi(itemid1:String ,itemid2:String , var similar:Double) extends Serializable

