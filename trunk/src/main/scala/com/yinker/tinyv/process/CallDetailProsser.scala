package com.yinker.tinyv.process

import com.yinker.tinyv.utils.DateUtils
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * Created by think on 2017/9/5.
  */

object CallDetailProsser {

  /**
    *
    * @return 平均六个月呼出数量
    */


  //  mobile
  //  user_id
  //  channel
  //  creditId
  //  creditTime
  //  List[mobile,CallingOrCalled，callTime]
  //  List[mobile]
  //  List[mobile]
  //  List[mobile]
  def c180ExhaleCountHalfyear(
                               creditInfoRdd: RDD[
                                 (
                                   String, //mobile 1
                                     String, //  user_id 2
                                     String, //  channel 3
                                     String, //  creditId 4
                                     String, //  creditTime 5
                                     String, //creditStatus 6
                                     Array[(String, String, String)], //  List[mobile,CallingOrCalled，callTime] 7
                                     Array[String], //  List[mobile] 8
                                     Array[String], //  List[mobile] 9
                                     Array[String]) //  List[mobile] 10

                                 ]): RDD[(String, String)] = {


    creditInfoRdd.map(x => {
      //授信时间
      val startTime = x._5.toLong
      val beforeLocal180Time = DateUtils.getUnixTimeBeforeDaysToTime(x._5, 180).toLong
      (
        s"${x._4}_${x._3}",
        (x._7.count(y => {
          val timePoint = y._3.toLong
          y._2 == "012001001" && (timePoint >= beforeLocal180Time && timePoint <= startTime)
        }) / 6.0).formatted("%.2f"))
    })
  }


  /*    callDetailsAndContactRDD.map(x => {
        val startTime = x._2._2.toLong
        val beforeLocal180Time = DateUtils.getUnixTimeBeforeDaysToTime(x._2._2, 180).toLong
        (x._1, (x._2._1._2.count(y => {
          val timePoint = y._3.toLong
          y._2 == "012001001" && (timePoint >= beforeLocal180Time && timePoint <= startTime)
        }) / 6.0).formatted("%.2f"))
      })
    }*/

  /**
    *
    * @return 贷后邦_通话记录三月以上呼叫人数
    */
  def stableContract3mthCount(
                               //                               callDetailsAndContactRDD: RDD[
                               //                                 (String, //k
                               //                                   ((
                               //                                     String, //mobile
                               //                                       Array[(
                               //                                         String, //o_mobile
                               //                                           String, //o_callling_or_else
                               //                                           String //o_create_time
                               //                                         )],
                               //                                       Array[String], //contact
                               //                                       Array[String], //calls_banks
                               //                                       Array[String]), //contact_banks
                               //                                     String //mobile_credit_time
                               //                                     ))]

                               creditInfoRdd: RDD[
                                 (
                                   String, //mobile 1
                                     String, //  user_id 2
                                     String, //  channel 3
                                     String, //  creditId 4
                                     String, //  creditTime 5
                                     String, //creditStatus 6
                                     Array[(String, String, String)], //  List[mobile,CallingOrCalled，callTime] 7
                                     Array[String], //  List[mobile] 8
                                     Array[String], //  List[mobile] 9
                                     Array[String]) //  List[mobile] 10

                                 ]

                             ): RDD[(String, Int)] = {


    //计算通话发生和客户授信时间的时间差   （差值为多少天，最小粒度--->天）
    def timeWindow(msecDiff: Long): Long = {
      val days = math.ceil(math.abs(msecDiff).toDouble / (1000 * 3600 * 24))
      //      math.ceil(().toDouble / (1000 * 3600 * 24)).toLong
      if (days > 0 && days <= 30) 1
      else if (days > 30 && days <= 60) 2
      else if (days > 60 && days <= 90) 3
      else if (days > 90 && days <= 120) 4
      else if (days > 120 && days <= 150) 5
      else 6
    }


    creditInfoRdd.map(x => {
      (
        s"${x._4}_${x._3}",
        x._5,
        x._7,
        x._1
      )
    })

      //      k               mobile callType callTime  CreditTime
      // RDD[(String, (Array[(String, String, String)], String))]
      .map(y => {
      //客户授信申请时间
      val CreditTime = y._2.toLong
      val befor180Days = DateUtils.getUnixTimeBeforeDaysToTime(y._2, 180).toLong
      val res = y._3.filter(z => {
        //过滤条件 主叫 && 通话时间在客户授信时间之前半年内
        //        println(s"k ${y._1}    time   ${z._3.toLong}")
        z._2 == "012001001" && (CreditTime >= z._3.toLong && z._3.toLong >= befor180Days)
      })
        //处理详单数据
        .map(m => {
        //将符合条件的所有详单中的手机号都打上标记（mobile,通话发生时间距客户授信1/2/3/4/5/6个月）

        val days = timeWindow(CreditTime - m._3.toLong)
        (m._1, days)
      })
        //按照手机号分组，相当于客户给单个人打电话的所有记录会被聚合
        .groupBy(_._1)
        //一个月中只要有一条通话记录就算当月是活跃联系人 不然需要按月在分组，去重的话是一个巧妙的办法
        .count(_._2.distinct.length >= 3)
      //        map中的k是手机号，做聚合用的，取活跃用户只用value就可以
      //        取出所有符合条件的对端号码的个数

      //返回通话记录三月以上呼叫人数
      (y._1, res)

    })
    /*
       callDetailsAndContactRDD.map(x => {
         //k   mobile_credit_time  calls  mobile
         //      println(s"${x._1}, (${x._2._2}, ${x._2._1._2}, ${x._2._1._1}")
         (x._1, (x._2._2, x._2._1._2, x._2._1._1))
       })

         //      k               mobile callType callTime  CreditTime
         // RDD[(String, (Array[(String, String, String)], String))]
         .map(y => {
         //客户授信申请时间
         val CreditTime = y._2._1.toLong
         val befor180Days = DateUtils.getUnixTimeBeforeDaysToTime(y._2._1, 180).toLong
         val res = y._2._2.filter(z => {
           //过滤条件 主叫 && 通话时间在客户授信时间之前半年内
           //        println(s"k ${y._1}    time   ${z._3.toLong}")

           z._2 == "012001001" && (CreditTime >= z._3.toLong && z._3.toLong >= befor180Days)
         })
           //处理详单数据
           .map(m => {
           //将符合条件的所有详单中的手机号都打上标记（mobile,通话发生时间距客户授信1/2/3/4/5/6个月）

           val days = timeWindow(CreditTime - m._3.toLong)
           (m._1, days)
         })
           //按照手机号分组，相当于客户给单个人打电话的所有记录会被聚合
           .groupBy(_._1)
           //一个月中只要有一条通话记录就算当月是活跃联系人 不然需要按月在分组，去重的话是一个巧妙的办法
           .count(_._2.distinct.length >= 3)
         //        map中的k是手机号，做聚合用的，取活跃用户只用value就可以
         //        取出所有符合条件的对端号码的个数

         //返回通话记录三月以上呼叫人数
         (y._1, res)

       })*/
  }

  /**
    * @return 通讯录内存储的无重复号码个数
    */
  def nonredundantCallNum(
                           /*callDetailsAndContactRDD: RDD[(
                             String, //mobile 1
                               String, //userId 2
                               String, //channel 3
                               Array[(String, String, String)], //callDettails (mobile callingOrCalled time)4 //@WILL time need change to unix time
                               Array[String], //contacts 5
                               Array[String], //calldetailsBankPhone 6
                               Array[String] //contactBankPhone 7
                             )]*/
                           creditInfoRdd: RDD[
                             (
                               String, //mobile 1
                                 String, //  user_id 2
                                 String, //  channel 3
                                 String, //  creditId 4
                                 String, //  creditTime 5
                                 String, //creditStatus 6
                                 Array[(String, String, String)], //  List[mobile,CallingOrCalled，callTime] 7
                                 Array[String], //  List[mobile] 8
                                 Array[String], //  List[mobile] 9
                                 Array[String]) //  List[mobile] 10
                             ]

                         ): RDD[(String, Int)] = {
    creditInfoRdd.filter(_._7.nonEmpty).map(x => {
      (s"${x._4}_${x._3}", x._8.distinct.length)

    })
  }

  /**
    * 时间窗口：全部详单，全部通讯录
    * 数据范围：数据范围：通讯录，通话详单里的手机号（需先进行手机号清洗）
    * 查询口径：注册手机号，银行卡预留手机号
    * 查询渠道：当前渠道
    * 计算口径：在时间窗口和数据范围内，计算当前渠道，在授信提交时间-7天内的申请授信的用户数（按渠道+userid去重）
    * 若无通讯录数据，仅计算通话详单数据，输出结果
    *
    * 通讯录/通话记录里过去7天内所有当前平台的人数
    *
    * @return
    */
  def appliedUsersInLast7days(
                               creditInfoRdd: RDD[
                                 (
                                   String, //mobile 1
                                     String, //  user_id 2
                                     String, //  channel 3
                                     String, //  creditId 4
                                     String, //  creditTime 5
                                     String, //creditStatus 6
                                     Array[(String, String, String)], //  List[mobile,CallingOrCalled，callTime] 7
                                     Array[String], //  List[mobile] 8
                                     Array[String], //  List[mobile] 9
                                     Array[String]) //  List[mobile] 10
                                 ]
                             ): RDD[(String, Int)] = {


    val flatMapRdd = creditInfoRdd.map(x => {
      //把四份数据进行union
      val allMobile: Array[String] = x._7.map(_._1) ++ x._8 ++ x._9 ++ x._10
      //并集进行去重 最小粒度 --> mobile
      val mobiledistinct = allMobile.distinct
      //creditID——channel   mainUserID,mainMobile, mainMobileChannel, mainMobileCreateTime, mainMobileCreateTimemobileArray
      (s"${x._4}_${x._3}", x._2, x._1, x._3, x._5, mobiledistinct)
    }).flatMap { case (u, v, w, x, y, z) =>
      //  Omobile   mainUserID,mainMobile, mainMobileChannel, mainMobileCreateTime
      z.map((_, (u, v, w, x, y)))
    }
    val OtherMobileRdd = creditInfoRdd.map(x => {
      (
        x._1, //otherMobile
        (
          x._1, //otherMobile
          x._2, // otherMObileUserID
          x._3, //otherMObileChannel
          x._4, //otherMobileCreateTime
          x._5) //otherMobileCreditStatus
      )
    })
    //    OtherMobileRdd.foreach(println)

    OtherMobileRdd.join(flatMapRdd).map(x => {
      (
        x._2._2._1, //creditID_channel
        (x._2._1._1, //otherMobile 1
          x._2._1._2, //otherMObileUserID 2
          x._2._1._3, //otherMObileChannel 3
          x._2._1._4, //otherMobileCreateTime 4
          x._2._1._5, //otherMobileCreditStatus 5
          x._2._2._2, //mainUserID 6
          x._2._2._3, //mainMobile 7
          x._2._2._4, //mainMobileChannel 8
          x._2._2._5))
    }) //mainMobileCreateTime 9

      .combineByKey(
      List(_),
      (x: List[(String, String, String, String, String, String, String, String, String)],
       y: (String, String, String, String, String, String, String, String, String)) => y +: x,
      (x: List[(String, String, String, String, String, String, String, String, String)],
       y: List[(String, String, String, String, String, String, String, String, String)]) => x ++ y)
      .map(x => {

        val beforeTime = DateUtils.getUnixTimeBeforeDaysToTime(x._2.head._9, 7).toLong

        val nowTime = x._2.head._9.toLong
        val appliedUsersInLast7daysCount = x._2.map(y => {
          //oUserID oChannel oCreditTime mainChannel mainCreditTime
          (y._2, y._3, y._4, y._8, y._9)
        }).filter(z => {

          val oCreditTime = z._5.toLong
          //判断条件
          //在时间窗口之内   （-7天内）
          nowTime >= oCreditTime && oCreditTime >= beforeTime &&
            //判断详单手机号的注册渠道为客户的授信申请渠道
            (z._2 == z._4)
          //判断详单手机号的注册渠道为客户的授信申请渠道或者'weibo'
          //          (z._2 == z._4 || z._2 == "weibo")
        }).map(m => {
          //只保留1个字段用于去重,上边已经唯一确定了渠道等于客户渠道 故而渠道唯一
          //oUserID
          m._1
        })
          //转换为map 进行去重 size的大小即为符合条件的人的个数（userId + Channel 去重）
          .distinct.size
        (x._1, appliedUsersInLast7daysCount)
      })


  }


  /**
    *
    * 时间窗口：全部详单，全部通讯录
    * 数据范围：通讯录，通话详单里的手机号（需先进行手机号清洗）
    * 查询口径：注册手机号，银行卡预留手机号
    * 查询时间：授信提交时间-30天
    * 计算口径：在时间窗口和数据范围内，在我司所有渠道，授信提交时间-30天已放款成功的用户数（按渠道+userid去重）。
    * *
    * 手机号的定义和如何清洗：
    * 在爬取到详单后，对详单中的对端号码进行清洗，清洗流程如下：
    * a) 从号码的最后一位向前取11位
    *i. 若可以取到11位，取11位数字
    *ii. 若不足11位，不处理，保留全部号码位数
    * b) 判断取到的11位是否是手机号
    *i. 判断逻辑：前2位是13，14，15， 17，18
    * *
    * 注意事项：
    * 若无数据，输出0
    * 若无通讯录数据，仅计算通话详单数据，输出结果
    *
    * @return 30天内成功放款人数
    */
  def successfulLoansInLast30days(
                                   loanInfoRDD: RDD[(String, String, String, String)],
                                   //                                   detailsAndContactMixDistinct: RDD[(String, (String, String, String, String))],
                                   creditInfoRdd: RDD[
                                     (
                                       String, //mobile 1
                                         String, //  user_id 2
                                         String, //  channel 3
                                         String, //  creditId 4
                                         String, //  creditTime 5
                                         String, //creditStatus 6
                                         Array[(String, String, String)], //  List[mobile,CallingOrCalled，callTime] 7
                                         Array[String], //  List[mobile] 8
                                         Array[String], //  List[mobile] 9
                                         Array[String]) //  List[mobile] 10
                                     ]
                                 ): RDD[(String, Int)] = {


    val flatMapRdd = creditInfoRdd.map(x => {
      //把四份数据进行union
      val allMobile: Array[String] = x._7.map(_._1) ++ x._8 ++ x._9 ++ x._10
      //并集进行去重 最小粒度 --> mobile
      val mobiledistinct = allMobile.distinct
      //creditID——channel   mainUserID,mainMobile, mainMobileChannel, mainMobileCreateTime, mainMobileCreateTimemobileArray
      (s"${x._4}_${x._3}", x._2, x._1, x._3, x._5, mobiledistinct)
    }).flatMap { case (u, v, w, x, y, z) =>
      //  Omobile  k mainUserID,mainMobile, mainMobileChannel, mainMobileCreateTime
      z.map((_, (u, v, w, x, y)))
    }
    val allLoanRDD = loanInfoRDD.map(x => {
      //oMobile (oUserId,oChannel,loanCreateTime)
      (x._1, (x._2, x._3, x._4))
    })


    val allJoinRDD = flatMapRdd.leftOuterJoin(allLoanRDD)

    allJoinRDD
      //取出有数据的客户
      .filter(_._2._2.nonEmpty)
      //按照用户userId + channel 进行唯一性区分
      .map(x => {
      (x._2._1._1, //s"${creditId}_$Channel",
        (
          x._2._1._2, //mainMobileUserID 用户渠道的唯一性ID 1
          x._1, //Omobile 基准手机号 2
          x._2._1._4, //mobileChannel 基准手机号渠道 3
          x._2._1._5, //mobileCreditTime 基准手机号详单授信申请时间 4
          x._2._2.get._1, //otherMobileUserId 对端手机对端手机号号user_id 5
          x._2._2.get._2, //otherMobileChannel  6
          x._2._2.get._3) //otherMobileLoanTime 对端手机放款时间 7
      )
    })
      //将所有借款事件按照客户的creditId + channel 进行聚合
      .combineByKey(
      List(_),
      (x: List[(String, String, String, String, String, String, String)],
       y: (String, String, String, String, String, String, String)) => y +: x,
      (x: List[(String, String, String, String, String, String, String)],
       y: List[(String, String, String, String, String, String, String)]) => x ++ y)
      .map(x => {
        //授信申请时间-30天
        val beforeTime = DateUtils.getUnixTimeBeforeDaysToTime(x._2.head._4, 30).toLong
        //当前时间
        val nowTime = x._2.head._4.toLong

        val successfulLoansInLast30daysLength = x._2
          //逻辑判断部分
          .filter(y => {
          //          println(nowTime + " " + y._7.toLong + " " + beforeTime)

          if (nowTime >= y._7.toLong && y._7.toLong >= beforeTime) {
            println(s"$nowTime >= ${y._7.toLong} && ${y._7.toLong} >= $beforeTime")
          } else {
            println(s"不匹配 ${DateUtils.unixTimeTotime(nowTime.toString)} >= ${DateUtils.unixTimeTotime(y._7)} ${DateUtils.unixTimeTotime(y._7)} >= ${DateUtils.unixTimeTotime(beforeTime.toString)}")
          }
          //过滤出时间窗口内的事件
          nowTime >= y._7.toLong && y._7.toLong >= beforeTime
        }).map(z => {
          //只取user_id + channel 进行唯一性区分
          s"${z._5}_${z._6}"
        })
          //去重（相同渠道放款成功多次，算作一次）后，list的元素个数即为成功放款人数
          .distinct.size

        //30天内成功放款人数不为0的人的rdd
        //        (x._1, -1)
        (x._1, successfulLoansInLast30daysLength)
      }).union(
      //无数据，返回0 的rdd
      //union的结果即为 30天内成功放款人数的rdd
      allJoinRDD.filter(_._2._2.isEmpty).map { x => (x._1, 0) }
    )
  }

  /** OK
    *
    * 时间窗口：全部详单，全部通讯录
    * 数据范围：通讯录，通话详单里的手机号（需先进行手机号清洗）
    * 查询口径：注册手机号，银行卡预留手机号
    * 计算口径：在时间窗口和数据范围内，
    * 在我司所有渠道的当前逾期和历史逾期内查询，输出：通讯录里逾期>7天用户数+通话详单里逾期>7天用户数/通讯录里放款成功用户数+通话详单里放款成功用户数
    * *
    * 注意事项：
    *1.用户数按平台的userid去重。同一平台内userid相同做去重，不同平台即为不同的用户
    *2.当通讯录里放款成功用户数+通话详单里放款成功用户数<=3时，输出0
    *3.若无数据可进行计算，输出空
    *4.若无通讯录数据，仅计算通话详单数据，输出结果
    *
    * @return 通话记录里历史放款申请的7天以上逾期率（分母3个以下设为0）
    */
  def overdueUsersAbove7days(
                              rdd: RDD[(String, List[(String, String, String, String, String, String, String, String, String, Int)])]): RDD[(String, String)] = {


    //聚合结果
    rdd.map(x => {
      //客户的授信申请时间
      val creditDay = x._2.head._4.toLong

      val overDuePerson = x._2.map(y => {

        //O_userID_O_channel loan_create_time overDueday
        (s"${y._5}_${y._7}", y._8, y._10)
      })
        //详单中所有人的已经有贷后表现的订单信息 按照详单中的creditId和channel进行聚合  得到的是一个map  Map[K, List[(String, String, Int)]]
        .groupBy(_._1)


      //逻辑：如果详单中有还款表现的人数小于3  认为逾期率为0
      if (overDuePerson.size <= 3) {
        (x._1, "0.0")
      } else {
        val overDuePersonMap = overDuePerson.map(z =>
          (
            // 唯一标识   user_id + channel
            z._1,
            // 过滤掉未来事件
            z._2
              .filter(_._2.toLong <= creditDay)
              //按照单个人的最大的逾期天数，找到最大的一个逾期事件
              .minBy(_._3)
              //拿到单个人的最大逾期天数
              ._3
          )
        )

        //详单中所有有还款表现的人的最大逾期天数大于七天的人 的总数
        val overThenSevenDaysNumber = overDuePersonMap.count(_._2 > 7)

        //返回用户 需求逻辑的计算结果(xxxxy逾期率)
        (x._1, (overThenSevenDaysNumber.toDouble / overDuePersonMap.size).formatted("%.2f"))
      }
    })
  }


  /** OK
    *
    * 通讯录和通话详单中重合的手机号，
    * 分别按注册手机号和银行卡预留手机号查询，
    * 查询这些手机号在我司所有平台中的逾期天数，
    * 在所有逾期天数中取最大值天数输出。
    *
    * 若无逾期，则输出空
    *
    * @return 并集中的最大逾期天数
    */
  def maxOverdueDaysInCallsAndContacts(rdd: RDD[
    (String,
      List[(String, String, String, String, String, String, String, String, String, Int)])]): RDD[(String, Int)] = {

    //得到详单中的所有人的逾期信息(最小粒度 loan_id) 按照客户的userID和channel聚合
    rdd.map(x => {

      //声明变量 拿到客户的授信申请时间 因为所有的详单中的人的详情里的客户授信时间是一致的，所有取一条即可
      val creditDay = x._2.head._4.toLong

      //存储所有的符合条件的订单信息
      var listBuffer = ListBuffer.empty[Int]

      //拿到用户详单人的每一笔订单信息，按照详单中人的userID+channel进行聚合
      x._2.map(y => {
        //O_userID O_channer loan_create_time overDueday
        (s"${y._5}_${y._7}", y._8, y._10)
      })

        //详单中所有人的已经有贷后表现的订单信息 按照详单中的userId和channel进行聚合  得到的是一个map  Map[K, List[(String, String, Int)]]
        .groupBy(_._1)


        .map(z => {
          //过滤掉借款事件发生时间在当前客户授信申请时间之后的借款事件
          z._2.filter(m => {
            val loanCreateTime = m._2.toLong
            //把借款创建时间在用户授信申请时间之前的借款事件拿到（包括未到还款日提前还款的人）
            loanCreateTime <= creditDay
          }).map(n => {
            //把所有符合条件的人的有还款表现的借款事件的逾期天数 放到listBuffer中
            listBuffer :+= n._3
            n
          })
        })
      val maxOverDueDay = listBuffer.max
      //返回listBuffer中所有逾期天数最大的一个 如无逾期 返回0
      if (maxOverDueDay <= 0) (x._1, 0) else (x._1, maxOverDueDay)
    })
  }

  /** OK
    * 详单和通讯录并集 之后聚合
    *
    * @return userId 1
    *         mobie 2
    *         channel 3
    *         mobileCreditTime 4
    *         otherMobileUserId 5
    *         otherMobileLoanId 6
    *         otherMobileChannel 7
    *         otherMobileCreateTime 8
    *         otherMobileDueDay 9
    *         otherMobileLoanOverDueDay 10
    *
    *         主要是拿到并集中的人中有借款，并且有还款表现得人的逾期天数 （提前还款也是有还款表现的，所有要用借款事件的创建时间区分，而不是还款时间）
    */
  def overDueDayCommon(
                        userRepayInfoRDD: RDD[(String, (String, String, String, String, String, Int))],
                        creditInfoRdd: RDD[
                          (
                            String, //mobile 1
                              String, //  user_id 2
                              String, //  channel 3
                              String, //  creditId 4
                              String, //  creditTime 5
                              String, //creditStatus 6
                              Array[(String, String, String)], //  List[mobile,CallingOrCalled，callTime] 7
                              Array[String], //  List[mobile] 8
                              Array[String], //  List[mobile] 9
                              Array[String]) //  List[mobile] 10
                          ]): RDD[(String, List[(String, String, String, String, String, String, String, String, String, Int)])] = {


    creditInfoRdd.map(x => {
      //把四份数据进行union
      val allMobile: Array[String] = x._7.map(_._1) ++ x._8 ++ x._9 ++ x._10
      //并集进行去重 最小粒度 --> mobile
      val mobiledistinct = allMobile.distinct
      //creditID——channel   mainUserID,mainMobile, mainMobileChannel, mainMobileCreateTime, mainMobileCreateTimemobileArray
      (s"${x._4}_${x._3}", x._2, x._1, x._3, x._5, mobiledistinct)
    }).flatMap { case (u, v, w, x, y, z) =>
      //  Omobile   mainUserID,mainMobile, mainMobileChannel, mainMobileCreateTime
      z.map((_, (u, v, w, x, y)))
    }.join(userRepayInfoRDD)
      .map(x => {
        (x._2._1._1, //s"${creditID}_$Channel"
          (
            x._2._1._3, //mobie 1
            x._2._1._2, //userId 2
            x._2._1._4, //channel 3
            x._2._1._5, //mobileCreditTime 4
            x._2._2._1, //otherMobileUserId 5
            x._2._2._3, //otherMobileLoanId 6
            x._2._2._2, //otherMobileChannel 7
            x._2._2._4, //otherMobileCreateTime 8
            x._2._2._5, //otherMobileDueDay 9
            x._2._2._6 //otherMobileLoanOverDueDay 10
          )
        )
      })

      //的到全量还款信息之后，客户（creditId +  channel） 进行聚合
      .combineByKey(
      List(_),
      (x: List[(String, String, String, String, String, String, String, String, String, Int)],
       y: (String, String, String, String, String, String, String, String, String, Int)) => y +: x,
      (x: List[(String, String, String, String, String, String, String, String, String, Int)],
       y: List[(String, String, String, String, String, String, String, String, String, Int)]) => x ++ y)

    /*    //通话详单join 全量用户还款信息的到详单中在我司有过借款记录且有还款表现的还款信息
        detailsAndContactMixDistinct.join(userRepayInfoRDD)

          .map(x => {
            (s"${x._2._1._2}_${x._2._1._3}", //s"${userID}_$Channel"
              (
                x._2._1._1, //mobie 1
                x._2._1._2, //userId 2
                x._2._1._3, //channel 3
                x._2._1._4, //mobileCreditTime 4
                x._2._2._1, //otherMobileUserId 5
                x._2._2._3, //otherMobileLoanId 6
                x._2._2._2, //otherMobileChannel 7
                x._2._2._4, //otherMobileCreateTime 8
                x._2._2._5, //otherMobileDueDay 9
                x._2._2._6 //otherMobileLoanOverDueDay 10
              )
            )
          })

          //的到全量还款信息之后，客户（userID +  channel） 进行聚合
          .combineByKey(
          List(_),
          (x: List[(String, String, String, String, String, String, String, String, String, Int)],
           y: (String, String, String, String, String, String, String, String, String, Int)) => y +: x,
          (x: List[(String, String, String, String, String, String, String, String, String, Int)],
           y: List[(String, String, String, String, String, String, String, String, String, Int)]) => x ++ y)*/
  }

  /**
    *
    * @param detailsAndContactMixDistinct 展开的list(mobile)
    * @param userCreditInfoRDD            用户授信信息
    * @return 聚合上边二者
    *
    */
  def creditCommon(
                    //                                     k
                    detailsAndContactMixDistinct: RDD[(String, (String, String, String, String))],
                    userCreditInfoRDD: RDD[(String, (String, String, String, String))]
                  ): RDD[(String, (String, String, String, String, String, String, String, String, String))] = {
    //通话详单和通讯录的并集去重
    detailsAndContactMixDistinct.join(userCreditInfoRDD)
      //详单中申请过授信的手机号,
      // (
      //    (详单所有者手机号,详单所有者授信申请渠道,详单所有者详单爬取时间),
      //    (基准手机号爬取渠道,爬取时间)
      // )
      .map(x => {
      (s"${x._2._1._2}_${x._2._1._3}", //s"${mainUserId}_${mainMobileChannel}"
        (
          x._1, //otherMobile  1
          x._2._2._1, //otherMobileUserID 2
          x._2._2._2, //otherMObileChannel 3
          x._2._2._3, //otherMobileCreateTime unixTime 4
          x._2._2._4, //otherMobileCreditStatus 5
          x._2._1._1, //mainMobile 6
          x._2._1._2, //mainUserId 7
          x._2._1._3, //mainMobileChannel 8
          x._2._1._4 //mainMobileCreateTime  unixTime 9
        ))
    })
  }


  /** ---- not OK
    *
    * 时间窗口：全部详单，全部通讯录
    * 数据范围：通讯录，通话详单里的手机号（需先进行手机号清洗）
    * 查询口径：注册手机号，银行卡预留手机号
    * 查询渠道：当前渠道
    * 计算口径：
    *1.在时间窗口和数据范围内，查询以下数据：
    *A.在通话详单的手机号中，授信提交时间-30天内，当前渠道，授信通过的用户数（按userid去重）
    *B.在通讯录的手机号中，授信提交时间-30天内，当前渠道，授信通过的用户数（按userid去重）
    *C.在通话详单的手机号中，授信提交时间-30天内，当前渠道，提交授信的用户数（按userid去重）
    *D.在通讯录的手机号中，授信提交时间-30天内，当前渠道，提交授信的用户数（按userid去重）
    *2.计算完成后，A和B去重，C和D去重
    *3.取当前渠道回溯的过去1个月平均授信通过率E
    *3.最终输出：[(A+B)/(C+D)]/E
    * 注意事项：
    *1.用户数按平台的userid去重。同一平台内userid相同做去重，不同平台即为不同的user。
    *2.当C+D<=3时，结果输出1
    *3.若无数据可计算，输出空
    *4.若无通讯录数据，仅计算通话详单数据，输出结果
    *
    * rdd 中间处理结果 对详单中每一条进行增加授信信息
    *
    * @return 过去1个月通讯录/通话记申请客户的通过率除以过去一个月平均通过率
    */
  def passRatioInLastMonth(
                            creditInfoRdd: RDD[
                              (
                                String, //mobile 1
                                  String, //  user_id 2
                                  String, //  channel 3
                                  String, //  creditId 4
                                  String, //  creditTime 5
                                  String, //creditStatus 6
                                  Array[(String, String, String)], //  List[mobile,CallingOrCalled，callTime] 7
                                  Array[String], //  List[mobile] 8
                                  Array[String], //  List[mobile] 9
                                  Array[String]) //  List[mobile] 10
                              ],
                            //             channel_day, pass_rate
                            pass30AvgRate: RDD[(String, String)]

                          ): RDD[(String, String)] = {
    val flatMapRdd = creditInfoRdd.map(x => {
      //把四份数据进行union
      val allMobile: Array[String] = x._7.map(_._1) ++ x._8 ++ x._9 ++ x._10
      //并集进行去重 最小粒度 --> mobile
      val mobiledistinct = allMobile.distinct
      //creditID——channel   mainUserID,mainMobile, mainMobileChannel, mainMobileCreateTime, mainMobileCreateTimemobileArray
      (s"${x._4}_${x._3}", x._2, x._1, x._3, x._5, mobiledistinct)
    }).flatMap { case (u, v, w, x, y, z) =>
      //  Omobile   mainUserID,mainMobile, mainMobileChannel, mainMobileCreateTime
      z.map((_, (u, v, w, x, y)))
    }

    val OtherMobileRdd = creditInfoRdd.map(x => {
      (
        x._1, //otherMobile
        (
          x._1, //otherMobile
          x._2, // otherMObileUserID
          x._3, //otherMObileChannel
          x._5, //otherMobileCreateTime
          x._6) //otherMobileCreditStatus
      )
    })


    val clientPassRate = OtherMobileRdd.join(flatMapRdd).map { x =>
      (
        x._2._2._1, //creditID_channel
        (x._2._1._1, //otherMobile 1
          x._2._1._2, //otherMObileUserID 2
          x._2._1._3, //otherMObileChannel 3
          x._2._1._4, //otherMobileCreateTime 4
          x._2._1._5, //otherMobileCreditStatus 5
          x._2._2._3, //mainMobile  6
          x._2._2._2, //mainUserID 7
          x._2._2._4, //mainMobileChannel 8
          x._2._2._5))
    } //mainMobileCreateTime 9


      .combineByKey(
      List(_),
      (x: List[(String, String, String, String, String, String, String, String, String)],
       y: (String, String, String, String, String, String, String, String, String)) => y +: x,
      (x: List[(String, String, String, String, String, String, String, String, String)],
       y: List[(String, String, String, String, String, String, String, String, String)]) => x ++ y)

      .map(x => {
        //只取当前平台的申请事件
        val mainChannel = x._2.head._8
        val clientCreditTime = x._2.head._4.toLong
        val clientCreditTimeBefore30 = DateUtils.getUnixTimeBeforeDaysToTime(x._2.head._4, 30).toLong
        val oneClientAllOtherMobileCreditInfo = x._2.filter(y => {
          val oCreditTime = y._9.toLong
          y._8 == y._3 && (clientCreditTime >= oCreditTime && clientCreditTimeBefore30 <= oCreditTime)
        })
        val successCreditNum = oneClientAllOtherMobileCreditInfo.count(_._5 == "0")
        val allCreditNum = oneClientAllOtherMobileCreditInfo.size
        val yesterDay = DateUtils
          .unixTimeTotime(DateUtils.getUnixTimeBeforeDaysToTime(clientCreditTime.toString, 1))
          .replaceAll(" [0-9]+:[0-9]+:[0-9]+$", "")
          .replaceAll("-0", "/")


        val clientPassRate = if (allCreditNum > 3) {
          (successCreditNum.toDouble / allCreditNum).formatted("%.4f")
        }
        //分母在1-3之间  返回1
        else if (allCreditNum > 1 && allCreditNum <= 3) {
          "-888"
        }
        //分母为0 返回 -999  最后返回空串
        else {
          "-999"
        }
        (s"${mainChannel}_$yesterDay", (x._1, clientPassRate))

      })
    clientPassRate.join(pass30AvgRate).map(x => {
      val k = x._2._1._1
      val clientRate = x._2._1._2.toDouble
      val pass30Rate = x._2._2.toDouble
      val res = if (x._2._1._2 == "-888") {
        "1.00"
      }
      else if (x._2._1._2 == "-999") {
        ""
      }
      else {
        (clientRate / pass30Rate).formatted("%.2f")
      }
      //      println(k + "  =   " + res+ "  || "+ clientRate + "  =  "+ pass30Rate)
      (k, res)
    })
  }


  /** OK
    * 取当前用户详单中的所有手机号（需经过清洗）
    * 这些手机号中取在我司平台有过授信申请记录的
    * 在以上取到的手机号中，比较通话记录的相似度（对端联系号码的重合度）
    * 取其中相似程度最高的5个号码，输出其中逾期天数最大的该天数
    *
    * @param userRepayInfoRDD 用户还款信息RDD
    * @param creditInfoRdd    授信事件详情RDD
    * @return (creditK,res)
    */

  def contactsSemblanceTop5Num(
                                //                      mobile  userId  channel  loanId  ,loanCreateTime,dueDay,overDueDay
                                userRepayInfoRDD: RDD[(String, (String, String, String, String, String, Int))],
                                creditInfoRdd: RDD[
                                  (
                                    String, //mobile 1
                                      String, //  user_id 2
                                      String, //  channel 3
                                      String, //  creditId 4
                                      String, //  creditTime 5
                                      String, //creditStatus 6
                                      Array[(String, String, String)], //  List[mobile,CallingOrCalled，callTime] 7
                                      Array[String], //  List[mobile] 8
                                      Array[String], //  List[mobile] 9
                                      Array[String]) //  List[mobile] 10
                                  ]
                              ): RDD[(String, Int)] = {


    val onceDegree = creditInfoRdd.map(x => {
      (
        x._1, //mobile
        (
          s"${x._4}_${x._3}", // creditId_channel
          x._2, // userId
          x._3, //channel
          x._7 // callsDetail(x,x,x)
        )
      )
    })


    val callsInfo = creditInfoRdd.map(x => {
      (
        x._1, //mobile
        s"${x._4}_${x._3}", // creditId_channel
        x._2, // userId
        x._3, //channel
        x._7.map(_._1) // callsDetail(x,x,x)

      )
    }).flatMap {
      case (a, b, c, d, e) =>
        e.map((_, (a, b, c, d)))
    }

    //取出授信通过的手机号  最小粒度--> mobile 不区分渠道
    val creditNoChannelRDD = creditInfoRdd.filter(_._6 == "0")
      .map(x => {
        (x._1, 1)
      }).distinct()


    //只取逾期天数    最小粒度 -->  mobile   不区分渠道
    val userRepayNoChannelRDD = userRepayInfoRDD
      .map(repayInfo => {
        (repayInfo._1, repayInfo._2._6)
      })
      //按照mobile聚合
      .combineByKey(
      List(_),
      (x: List[Int], y: Int) => y +: x,
      (x: List[Int], y: List[Int]) => x ++ y
    )


    callsInfo.join(onceDegree)
      //只取出客户和他的二度联系人
      .map(x => {
      //userId_channel
      val oneK = s"${x._2._1._3}_${x._2._1._4}"
      //creditId_channel
      val oneCreditK = x._2._1._2
      //客户的通话详单的手机号
      //      val oneV = x._1
      //二度联系人
      val twoV = x._2._2
      val clientMobile = x._2._1._1
      (oneK, oneCreditK, clientMobile, twoV._4)
    })
      //展开二度联系人
      .flatMap {
      case (oneUserIdK, oneCreditK, clientMobile, twoDegreeMoibleList) =>
        twoDegreeMoibleList.map((_, (oneUserIdK, oneCreditK, clientMobile)))
    }.map(x => {
      //twoDegreeMobile oneUserIdK,oneCreditK ,clientMobile
      (x._1._1, (x._2._1, x._2._2, x._2._3))
    })
      //      //取出所有授信通过的人  粒度 -> mobile
      .join(creditNoChannelRDD)
      //      //取出已授信人所有有还款表现的人的最大逾期天数  粒度 -> mobile
      .leftOuterJoin(userRepayNoChannelRDD)

      //        oMobile      oneUserIdK,oneCreditK ,clientMobile  _(join占位)       List(overDueDay)
      //    RDD[(String, ((( String,      String,    String),        Int),        Option[List[Int]]))]


      /**
        * 因为他们通话人和对端号码互为一度联系人,
        * 所以二度联系人中一定有客户的手机号,
        * 而特征提取目的是用其他人评估客户,
        * 所以需要把客户的手机号过滤掉
        * ************************************
        * 需要先行filter 这样map的时候会少一个字段*
        * ************************************
        */

      .filter(p => p._2._1._1._3 != p._1)
      //格式化join的结果 用于后边数据的聚合
      .map(x => {

      //      val oneUserIdK = x._2._1._1._1
      val oneCreditK = x._2._1._1._2
      //      val clientMobile = x._2._1._1._3
      val oMobile = x._1
      val oMobileOverDueDayList = x._2._2
      val oMobileMaxOverDueDay = oMobileOverDueDayList match {
        case Some(_) => oMobileOverDueDayList.get.max
        case None => -999
      }
      //用userId_channel 唯一标识一个客户  去掉了 客户手机
      //string (string,int)
      (oneCreditK, (oMobile, oMobileMaxOverDueDay))
    })
      //只取二度
      .combineByKey(
      List(_),
      (x: List[(String, Int)], y: (String, Int)) => x :+ y,
      (x: List[(String, Int)], y: List[(String, Int)]) => x ++ y)
      .map(x => {
        val clientMobileMaxOverDueDay = x._2
          //二度详情
          .groupBy(_._1)
          //只取后边的value
          .values
          //添加相似度
          .map(y => {
          (y, y.size)
        })
          //需要排序，转为list
          .toList

          //相似度排序 升序
          .sortBy(_._2)
          //升序反转为降序
          .reverse
          //          .filter(_._1.head._1 !=)
          //取相似度最高的前五条
          .take(5)
          //    用于取相似度，一个人会出现在多人的详单中  相似度
          //        List[(List[(String, Int)],      Int)]
          .map {
          _._1.head._2
        }
          //取出最大的逾期天数
          .sorted.last
        //如果二度中  全是授信过但是未借款的  返回0
        if (clientMobileMaxOverDueDay == -999) {
          (x._1, 0)
        } else {
          //返回二度中符合需求的最大逾期天数
          (x._1, clientMobileMaxOverDueDay)
        }
      })

  }
}
