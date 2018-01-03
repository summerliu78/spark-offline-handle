00000001   [0, ]  渠道号 channel
01000001   [0, ]  性别 age
01000002   [0, ]  年龄 gender
01000012   [0, ]  户籍省 attribution2_province
01000018   [0, ]  手机户籍省是否一致
01000017   [0, ]  手机银行手机是否一致
01001000   [0, ]  同盾风险分 td_risk_score
01001001   [0, ]  同盾信用分 td_credit_score
01002000   [0, ]  蜜罐手机灰度值 mg_gray_score
01002003   [0, ]  蜜罐直接联系人数目 mg_contact_num
01002001   [0, ]  蜜罐间接联系人在黑名单的数量 mg_contact2_num
01002002   [0, ]  蜜罐被机构查询数量 mg_search_num
01004003   [0, ]  前海消费金融机构数 qh_institution_num
01004002   [0, ]  前海多头分数 qh_long_position_score
01005002   [0, ]  微博申请注册时间 wb_daydiff_number
01003076   [0, ]  近150天与紧急联系人联系次数 emergency_contact_call_num_150
01003077   [0, ]  近150天紧急联系人呼出次数 emergency_contact_out_num_150
01003078   [0, ]  近150天与紧急联系人号码的呼入时长 emergency_contact_in_time_150
01003008   [0, ]  近150天主叫总次数（近150天内呼出通话总次数） total_call_num_150
01003071   [0, ]  近150天内呼入电话占比  incoming_call_rate_150
01003069   [0, ]  近150天内呼出有效语音通话（通话时长>15秒）联系人数  valid_contacts_num_150
01003070   [0, ]  近150天内总有效语音通话（通话时长>15秒）联系人数  valid_contacts_num_out_150
01003067   [0, ]  近150天通话对象个数 contact_num_150
01003066   [0, ]  近180天内无通话天数 no_call_days_150
01003075   [0, ]  近150天通话详单中联系110数量 call_police_num
01003049   [0, ]  近150天内0~6点主叫联系人（去重）个数 dawn_contact_num_150
01003043   [0, ]  近150天内0~6点主叫次数 dawn_out_call_num_150
01003044   [0, ]  近150天内0~6点被叫次数 dawn_in_call_num_150
01003040   [0, ]  近30/150天被叫通话次数占比 incoming_call_rate_30_150
01003081   [0, ]  近30天内呼出联系人个数除以呼入联系人个数（去重） outgoing_incoming_contacts_rate_30
01001002   [0, ]  同盾6个月多平台 td_loan_times
01003068   [0, ]  手机在网时长 network_days



【用户基本信息】
select
	a.loan_id loan_id	--借款事件id
	,a.ds channel	--借款事件所属渠道
	,c.user_gender gender	--性别
	,c.user_age age	--年龄
	,b.mobile phone	--手机号
	,c.mob_province attribution1_province	--手机号归属地省份
	,c.user_province_id	attribution2_province --身份证省份ID	
	,a.overdue_day overdue_day	--借款事件当前逾期天数
	,a.repay_status mark	--还款状态
	,case when b.mobile = b.reserve_mobile then 0 else 1 end is_phone_same  --手机号与预留手机号是否一致
from	
	(select
		loan_id
		,user_id
		,ds
		,max(overdue_day) overdue_day	--分期产品时取最大逾期天数
		,repay_status	--还款状态
	from edw_tinyv.e_repay_plan_detail_d
	where dt = date_add(current_date(),-1)
		  and (to_date(due_day) <= date_add(current_date(),-1)
			   or
			   repay_status = '002005002')
	group by loan_id,user_id,ds,create_time,repay_status
	) a
join
	(select
		user_id
		,ds
		,mobile		--手机号
		,reserve_mobile		--银行卡预留手机号
	from ods_tinyv.o_fuse_user_info_contrast_d
	where dt = date_add(current_date(),-1)
	) b
on a.user_id = b.user_id and a.ds = b.ds
join
	(select
		user_id
		,ds
		,user_gender	--性别
		,user_age	--年龄
		,mobile_info['province'] mob_province	--手机号归属地省份
		,user_province_id	--身份证省份ID
	from edw_tinyv.e_user_integ_info_d
	where dt = date_add(current_date(),-1)
	) c
on a.user_id = c.user_id and a.ds = c.ds

--------------------------------------------------------------------------------------------------------

【借款事件关联蜜罐信息】
set hive.auto.convert.join=false;
select
	a.loan_id loan_id	--借款事件id
	,a.ds channel	--借款事件所属渠道
	,d.phone_gray_score	mg_gray_score --蜜罐黑中介 分
	,d.contacts_class1_cnt mg_contact_num	--一度联系人总数
	,d.contacts_class2_blacklist_cnt mg_contact2_num --二度联系人在黑名单数量
	,d.searched_org_cnt	mg_search_num --被机构查询数量
from	
	(select
		loan_id
		,ds
	from edw_tinyv.e_repay_plan_detail_d
	where dt = date_add(current_date(),-1)
		  and (to_date(due_day) <= date_add(current_date(),-1)
			   or
			   repay_status = '002005002')
	group by loan_id,ds
	) a
left join
	(select
		m.loan_id
		,m.ds
		,n.phone_gray_score	--蜜罐黑中介 分
		,n.contacts_class1_cnt	--一度联系人总数
		,n.contacts_class2_blacklist_cnt	--二度联系人在黑名单数量
		,n.searched_org_cnt	--被机构查询数量
	from 
		(select
			loan_id
			,ds
			,risk_id
		from bdw_tinyv.b_fuse_loan_risk_d m
		where dt = date_add(current_date(),-1)
			  and del_flag = 0
			  and risk_type = '004002005'
		group by loan_id,ds,risk_id
		) m
	join
		(select
			task_id
			,ds
			,phone_gray_score
			,contacts_class1_cnt
			,contacts_class2_blacklist_cnt
			,searched_org_cnt
		from bdw_tinyv_outer.b_fuse_mg_detail_i
		where dt <= date_add(current_date(),-1)
		group by task_id,ds,phone_gray_score,contacts_class1_cnt,contacts_class2_blacklist_cnt,searched_org_cnt
		) n
	on m.risk_id = n.task_id and m.ds = n.ds
	) d
on a.loan_id = d.loan_id and a.ds = d.ds

--------------------------------------------------------------------------------------------------------

【借款事件关联前海信息】
set hive.auto.convert.join=false;
select
	a.loan_id loan_id	--借款事件id
	,a.ds channel	--借款事件所属渠道
	,d.credooscore qh_long_position_score	--好信度评分
	,e.cnssamount qh_institution_num  --命中消费金融机构数
from
	(select
		m.loan_id
		,m.ds
		,n.risk_id
	from 
		(select
			loan_id
			,ds
		from edw_tinyv.e_repay_plan_detail_d
		where dt = date_add(current_date(),-1)
			  and (to_date(due_day) <= date_add(current_date(),-1)
			       or
			       repay_status = '002005002')
		group by loan_id,ds
		) m
	left join
		(select
			loan_id
			,ds
			,risk_id
		from bdw_tinyv.b_fuse_loan_risk_d
		where dt = date_add(current_date(),-1)
			  and del_flag = 0
			  and risk_type in ('004002007','004002008')
		) n
	on m.loan_id = n.loan_id and m.ds = n.ds
	group by m.loan_id,m.ds,n.risk_id
	) a
left join
	(select 
		order_no
		,ds
		,records['credooscore'] credooscore--前海好信度评分
	from bdw_tinyv_outer.b_fuse_qhcs_goodcredit_i
	lateral view explode(query_result.records) t as records 
	where dt <= date_add(current_date(),-1)
	) d
on a.risk_id = d.order_no and a.ds = d.ds
left join
	(select 
		order_no
		,ds
		,records['cnssamount'] cnssamount --命中消费金融机构数
	from bdw_tinyv_outer.b_fuse_qhcs_regularloan_i 
	lateral view explode(query_result.records) t as records 
	where dt <= date_add(current_date(),-1)
	) e
on a.risk_id = e.order_no and a.ds = e.ds
group by a.loan_id,a.ds,d.credooscore,e.cnssamount


--------------------------------------------------------------------------------------------------------

【第一次借款申请时间与微博注册时间差】
set hive.auto.convert.join=False;
select
	m.loan_id loan_id
	,'weibo' channel
	,datediff(to_date(m.a_t),to_date(n.wb_reg_time)) wb_daydiff_number
from
	(select
		x.loan_id
		,x.user_id
		,y.a_t
	from 
		(select
			loan_id
			,user_id
		from edw_tinyv.e_repay_plan_detail_d
		where dt = date_add(current_date(),-1)
			  and ds = 'weibo'
			  and (to_date(due_day) <= date_add(current_date(),-1)
			       or
			       repay_status = '002005002')
		group by loan_id,user_id,create_time
		) x
	join
		(select
			user_id
			,min(apply_time) a_t
		from edw_tinyv.e_loan_detail_info_d
		where dt = date_add(current_date(),-1)
			  and ds = 'weibo'
		group by user_id
		) y
	on x.user_id = y.user_id
	) m
left join
	(select 
		user_id
		,wb_reg_time
	from bdw_tinyv.b_fuse_user_info_d
	where dt = date_add(current_date(),-1)
		  and ds = 'weibo'
	) n
on m.user_id = n.user_id

--------------------------------------------------------------------------------------------------------
【手机号入网时间与本次借款时间差】
select
	x.loan_id loan_id
	,x.ds channel
	,x.open_day_cnt network_days
from
	(select
		a.loan_id
		,a.ds
		,case when b.open_date in ('null','','NULL') then -1
			  else datediff(to_date(a.create_time),b.open_date)
			  end open_day_cnt
	from
		(select
			m.loan_id
			,m.ds
			,m.create_time
			,n.mobile
		from
			(select
				loan_id
				,user_id
				,ds
				,create_time
			from edw_tinyv.e_repay_plan_detail_d
			where dt = date_add(current_date(),-1)
				  and (to_date(due_day) <= date_add(current_date(),-1)
			           or
			           repay_status = '002005002')
			group by loan_id,user_id,ds,create_time
			) m
		join
			(select
				user_id
				,ds
				,mobile		--手机号
			from ods_tinyv.o_fuse_user_info_contrast_d
			where dt = date_add(current_date(),-1)
			) n
		on m.user_id = n.user_id and m.ds = n.ds
		) a
	left join
		(select
			x.mobile
			,x.ds
			,x.open_date
		from
			(select
				case when crawler_channel = 'JuXinLi' then basic_info['cell_phone']
					 when crawler_channel = 'tongd' then basic_info['user_number']
					 end mobile
				,ds
				,case when crawler_channel = 'JuXinLi' then to_date(basic_info['reg_time'])
					  when crawler_channel = 'tongd' then basic_info['net_time']
					  end open_date
			from bdw_tinyv_outer.b_fuse_call_real_auth_i
			where dt <= date_add(current_date(),-1)
			) x
		group by x.mobile,x.ds,x.open_date
		) b
	on a.mobile = b.mobile and a.ds = b.ds
	) x
group by x.loan_id,x.ds,x.open_day_cnt


--------------------------------------------------------------------------------------------------------

【借款事件关联同盾信息】【分渠道】
set hive.auto.convert.join=false;
select
	a.loan_id loan_id	--借款id
	,'weibo' channel	--借款事件渠道
    ,b.a2624922 td_loan_times	--6个月申请机构数
from
	(select
		m.loan_id
		,n.risk_id
	from 
		(select
			loan_id
		from edw_tinyv.e_repay_plan_detail_d
		where dt = date_add(current_date(),-1)
			  and ds = 'weibo'
			  and (to_date(due_day) <= date_add(current_date(),-1)
			       or
			       repay_status = '002005002')
		group by loan_id
		) m
	left join
		(select
			loan_id
			,risk_id
		from bdw_tinyv.b_fuse_loan_risk_d
		where dt = date_add(current_date(),-1)
			  and del_flag = 0
			  and ds = 'weibo'
			  and risk_type = '004002001'
		) n
	on m.loan_id = n.loan_id
	group by m.loan_id,n.risk_id
	) a
left join
	(select 
		td.sequence_id
		,sum(case when td.ruleid = '2624922' then td.cnt  else '' end) a2624922
	from
		(select 
			b.sequence_id
			,b.ruleid
			,hit['count'] as cnt 
		from
			(select 
				a.sequence_id
				,a.ruleid
				,condition.hits
			from
				(select 
					`_id` sequence_id
					,rules1.ruleid ruleid
					,rules1.conditions conditions
				from ods_tinyv_outer.weibo_td_detail_v31_mongo_i
				lateral view explode(rules) t as rules1
				where dt <= date_add(current_date(),-1)
				) a
			lateral view explode(conditions) a as condition
			) b
		lateral view explode(hits) b as hit
		) td
	group by td.sequence_id
	) b
on a.risk_id = b.sequence_id
group by a.loan_id,a.risk_id,b.a2624922



【同盾信用分，风险分】【分渠道】
set hive.auto.convert.join=false;
select
	a.loan_id loan_id	--借款id
	,'weibo' channel	--借款时间渠道
    ,b.final_score td_risk_score	--同盾风险分
	,b.credit_score td_credit_score		--同盾信用分
from
	(select
		m.loan_id
		,n.risk_id
	from 
		(select
			loan_id
		from edw_tinyv.e_repay_plan_detail_d
		where dt = date_add(current_date(),-1)
			  and ds = 'weibo'
			  and (to_date(due_day) <= date_add(current_date(),-1)
			       or
			       repay_status = '002005002')
		group by loan_id
		) m
	left join
		(select
			loan_id
			,risk_id
		from bdw_tinyv.b_fuse_loan_risk_d
		where dt = date_add(current_date(),-1)
			  and del_flag = 0
			  and ds = 'weibo'
			  and risk_type = '004002001'
		) n
	on m.loan_id = n.loan_id
	group by m.loan_id,n.risk_id
	) a
left join
	(select
		`_id` risk_id
		,final_score
		,credit_score['id_number_mobile_score'] credit_score
	from ods_tinyv_outer.weibo_td_mongo_i
	where dt <= date_add(current_date(),-1)
	) b
on a.risk_id = b.risk_id
group by a.loan_id,b.final_score,b.credit_score

--------------------------------------------------------------------------------------------------------

【借款成功且还款到期用户近150天通话详单】【和高伟碰一下情况！】【ds和loan_id是否加上】
select
	a.mobile mobile
	,a.ds ds
	,b.other_mobile other_mobile
	,b.call_channel call_channel
	,b.call_time call_time
	,b.call_datetime call_datetime
	,b.create_time create_time
	,b.call_fee call_fee
from
	(select
		m.ds
		,n.mobile
	from
		(select
			user_id
			,ds
		from edw_tinyv.e_repay_plan_detail_d
		where dt = date_add(current_date(),-1)
			  and (to_date(due_day) <= date_add(current_date(),-1)
			       or
			       repay_status = '002005002')
		group by user_id,ds
		) m
	join
		(select
			user_id
			,ds
			,mobile		--手机号
		from ods_tinyv.o_fuse_user_info_contrast_d
		where dt = date_add(current_date(),-1)
		) n
	on m.user_id = n.user_id and m.ds = n.ds
	) a
join
	(select
		x.mobile
		,x.ds
		,x.other_mobile
		,x.call_channel
		,x.call_time
		,x.call_datetime
		,x.create_time
		,x.call_fee
	from bdw_tinyv_outer.b_fuse_call_detail_i x
	join
		(select
			mobile
			,ds
			,max(create_time) ct
		from bdw_tinyv_outer.b_fuse_call_detail_i
		where 
			dt <= date_add(current_date(),-1)
			and to_date(call_datetime) >= date_add(to_date(create_time),-150)
		group by mobile,ds
		) y
	on x.mobile = y.mobile and x.ds = y.ds and x.create_time = y.ct
	where 
		  x.dt <= date_add(current_date(),-1)
		  and to_date(x.call_datetime) >= date_add(to_date(x.create_time),-150)
		  and x.call_channel  is not in ('null','','NULL')
		  and x.other_mobile is not in ('null','','NULL')
		  and x.call_time is not in ('null','','NULL')
		  and x.call_datetime is not in ('null','','NULL')
	) b
on a.mobile = b.mobile and a.ds = b.ds
group by 
	a.ds,a.mobile,b.other_mobile,b.call_channel,b.call_time,b.call_datetime,b.create_time,b.call_fee

-------------------------------------------------------------------------------------------------------	


【通话详单一体化SQL】
select
	ds channel
	,mobile
	,sum(case when call_channel = '012001001' then 1 else 0 end) total_call_num_150
	--近150天内呼出通话总次数
	,sum(case when call_channel = '012001002' then 1 else 0 end)/count(1) incoming_call_rate_150
	--近150天内呼入通话总次数占总通话次数比
	,count(distinct (case when call_time>15 and call_channel = '012001001' then other_mobile else 0 end))-1 valid_contacts_num_150
	--近150天内呼出有效语音通话（通话时长>15秒）联系人数
	,count(distinct (case when call_time>15 then other_mobile else 0 end))-1 valid_contacts_num_out_150
	--近150天内有效语音通话（通话时长>15秒）联系人数
	,count(distinct other_mobile) contact_num_150
	--近150天通话对象个数
	,150-count(distinct to_date(call_datetime)) no_call_days_150
	--近150天内无通话总天数
	,sum(case when other_mobile = '110' then 1 else 0 end) call_police_num
	--近150天通话详单中联系110数量
	,sum(case when to_date(call_datetime) >= date_add(to_date(create_time),-30) then call_fee else 0 end) fare_30
	--近30天手机通话费用
	,count(distinct (case when hour(call_datetime)<=6 then other_mobile else 0 end))-1  dawn_contact_num_150
	--近150天内0~6点联系人个数（去重）
	,sum(case when hour(call_datetime)<=6 and call_channel = '012001001' then 1 else 0 end) dawn_out_call_num_150
	--近150天内0~6点主叫次数
	,sum(case when hour(call_datetime)<=6 and call_channel = '012001002' then 1 else 0 end) dawn_in_call_num_150
	--近150天内0~6点被叫次数
	,(sum(case when 
			   to_date(call_datetime) >= date_add(to_date(create_time),-30) 
			   and call_channel = '012001002' 
			   then 1 
			   else 0 
			   end)-1)/
	 sum(case when 
			  call_channel = '012001002' 
			  then 1 
			  else 0 
			  end) incoming_call_rate_30_150
	--近30天被叫次数与近150天被叫次数比值
	,(count(distinct (case when 
						   to_date(call_datetime) >= date_add(to_date(create_time),-30) 
						   and call_channel = '012001001' 
						   then other_mobile 
						   else 0 
					       end))-1)/
	 (count(distinct (case when 
						   to_date(call_datetime) >= date_add(to_date(create_time),-30) 
						   and call_channel = '012001002' 
						   then other_mobile 
						   else 0 
					       end))-1) outgoing_incoming_contacts_rate_30
	--近30天内呼出联系人个数除以呼入联系人个数（去重）
from tinyv_analysis_db.loan_call_detail_150_v2
group by ds,mobile



【近150天呼入超过3次以上座机数量】
select
	c.mobile mobile
	,c.ds channel
	,count(1) incoming_landline_num_150  --近150天呼入超过3次以上座机数量
from
	(select
		a.ds
		,a.mobile
		,b.other_mobile
	from
		(select
			mobile
			,ds
		from tinyv_analysis_db.loan_call_detail_150_v2
		group by mobile,ds
		) a
	join
		(select
			mobile
			,ds
			,other_mobile
			,count(1) cnt
		from tinyv_analysis_db.loan_call_detail_150_v2
		where call_channel = '012001002'
			  and other_mobile not in ('null','','NULL')
			  and length(other_mobile) < 5
			  and other_mobile not in ('10086','10001','10000','10010')
			  and substr(other_mobile,1,2) != '95'
			  and substr(other_mobile,1,3) != '400'
		group by mobile,ds,other_mobile
		) b
	on a.mobile = b.mobile and a.ds = b.ds and b.cnt>3
	) c
group by c.mobile,c.ds



【紧急联系人更新】
select
	a.mobile mobile
	,a.ds channel
	,count(1) emergency_contact_call_num_150
	--近150天与紧急联系人联系次数
	,sum(case when a.call_channel = '012001001' then 1 else 0 end) emergency_contact_out_num_150
	--近150天紧急联系人呼出次数
	,sum(case when a.call_channel = '012001002' then a.call_time else 0 end) emergency_contact_in_time_150
	--近150天与紧急联系人号码的呼入时长
from tinyv_analysis_db.loan_call_detail_150_v2 a
left join
	(select
		mobile
		,ds
		,relation1_mobile emergency_mobile	--手机号
	from ods_tinyv.o_fuse_user_info_contrast_d
	where dt = date_add(current_date(),-1)
	union all
	select
		mobile
		,ds
		,relation2_mobile emergency_mobile	--手机号
	from ods_tinyv.o_fuse_user_info_contrast_d
	where dt = date_add(current_date(),-1)
	) b
on a.mobile = b.mobile and a.ds = b.ds and a.other_mobile = b.emergency_mobile
group by a.mobile,a.ds	--前提是a表中有loan_id和ds
