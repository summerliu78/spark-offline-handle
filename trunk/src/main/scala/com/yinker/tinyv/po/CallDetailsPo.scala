package com.yinker.tinyv.po

/**
  * Created by think on 2017/9/5.
  */
class CallDetailsPo extends Serializable {}

/**
  *
  * @param mobile      手机号
  * @param user_id     用户id
  * @param channel     用户渠道
  * @param callDetails 通话详单
  * @param contact     通讯录
  */
case class CallDetailsAndContact(
                                  mobile: String,
                                  user_id: String,
                                  channel: String,
                                  detailsCreatTime: String,
                                  var callDetails: Array[String],
                                  var contact: Array[String]
                                ) extends Serializable


/**
  *
  * @param mobile     手机号
  * @param userId     用户id
  * @param channel    用户渠道
  * @param creditTime 借款时间
  *
  */


//@WILL   所有时间开始点都是授信提交时间   也就是需要关联  credit_apply表
case class UserCreditInfo(
                           mobile: String,
                           userId: String,
                           channel: String,
                           var creditTime: String
                         ) extends Serializable

/**
  *
  * @param mobile     手机号
  * @param userId     用户ID
  * @param channel    借款渠道
  * @param loanId     借款id
  * @param overDueDay 逾期天数
  */
case class UserRepayInfo(
                          mobile: String,
                          userId: String,
                          channel: String,
                          loanId: String,
                          overDueDay: Int
                        ) extends Serializable