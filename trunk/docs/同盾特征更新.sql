【同盾风险分和信用分-所有平台】
set hive.auto.convert.join=false;
select
	a.loan_id loan_id	--借款id
	,a.ds channel		--借款时间渠道
    ,b.final_score td_risk_score		--同盾风险分
	,b.credit_score td_credit_score		--同盾信用分
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
			  and risk_type = '004002001'
		) n
	on m.loan_id = n.loan_id and m.ds = n.ds
	group by m.loan_id,m.ds,n.risk_id
	) a
left join
	(select
		`_id` risk_id
		,ds
		,final_score
		,credit_score['id_number_mobile_score'] credit_score
	from bdw_tinyv_outer.b_fuse_td_mongo_i
	where dt <= date_add(current_date(),-1)
	) b
on a.risk_id = b.risk_id and a.ds = b.ds
group by a.loan_id,a.ds,b.final_score,b.credit_score

【借款事件关联同盾6个月】【weibo】
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
		a.sequence_id
		,a.condition.result a2624922
	from
		(select 
			`_id` sequence_id
			,rules1.ruleid ruleid
			,rules1.conditions conditions
		from ods_tinyv_outer.weibo_td_detail_v31_mongo_i
		lateral view explode(rules) t as rules1
		where 
			dt < '2017-08-07'
		) a
	lateral view explode(a.conditions) a as condition
	where ruleid = '2624922'
	union all
	select 
		a.sequence_id
		,condition.result a2624922
	from
		(select 
			seqid sequence_id
			,rules1.rule_id ruleid
			,rules1.conditions conditions
		from ods_tinyv_outer.weibo_td_detail_v32_mongo_i
		lateral view explode(rawdata.rules) t as rules1
		where dt >= '2017-08-07'
		) a
	lateral view explode(a.conditions) a as condition
	where ruleid = '2624922'
	) b
on a.risk_id = b.sequence_id
group by a.loan_id,a.risk_id,b.a2624922

【借款事件关联同盾信息】【bbqb】
set hive.auto.convert.join=false;
select
	a.loan_id loan_id	--借款id
	,'bbqb' channel	--借款事件渠道
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
			  and ds = 'bbqb'
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
			  and ds = 'bbqb'
			  and risk_type = '004002001'
		) n
	on m.loan_id = n.loan_id
	group by m.loan_id,n.risk_id
	) a
left join
	(select 
		a.sequence_id
		,a.condition.result a2624922
	from
		(select 
			`_id` sequence_id
			,rules1.ruleid ruleid
			,rules1.conditions conditions
		from ods_tinyv_outer.bbpay_td_detail_v31_mongo_i
		lateral view explode(rules) t as rules1
		where 
			dt < '2017-08-14'
		) a
	lateral view explode(a.conditions) a as condition
	where ruleid = '2624922'
	union all
	select 
		a.sequence_id
		,condition.result a2624922
	from
		(select 
			seqid sequence_id
			,rules1.rule_id ruleid
			,rules1.conditions conditions
		from ods_tinyv_outer.univ_td_detail_v32_mongo_i
		lateral view explode(rawdata.rules) t as rules1
		where 
			dt >= '2017-08-14'
			and platformchannel = 'bbqb'
		) a
	lateral view explode(a.conditions) a as condition
	where a.ruleid = '2624922'
	) b
on a.risk_id = b.sequence_id
group by a.loan_id,a.risk_id,b.a2624922


【借款事件关联同盾6个月】【hao123】
set hive.auto.convert.join=false;
select
	a.loan_id loan_id	--借款id
	,'hao123' channel	--借款事件渠道
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
			  and ds = 'hao123'
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
			  and ds = 'hao123'
			  and risk_type = '004002001'
		) n
	on m.loan_id = n.loan_id
	group by m.loan_id,n.risk_id
	) a
left join
	(select 
		a.sequence_id
		,a.condition.result a2624922
	from
		(select 
			`_id` sequence_id
			,rules1.ruleid ruleid
			,rules1.conditions conditions
		from ods_tinyv_outer.hao123_td_detail_v31_mongo_i
		lateral view explode(rules) t as rules1
		where 
			dt <= date_add(current_date(),-1)
		) a
	lateral view explode(a.conditions) a as condition
	where a.ruleid = '2624922'
	) b
on a.risk_id = b.sequence_id
group by a.loan_id,a.risk_id,b.a2624922

【借款事件关联同盾6个月】【beike】
set hive.auto.convert.join=false;
select
	a.loan_id loan_id	--借款id
	,'beike' channel	--借款事件渠道
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
			  and ds = 'beike'
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
			  and ds = 'beike'
			  and risk_type = '004002001'
		) n
	on m.loan_id = n.loan_id
	group by m.loan_id,n.risk_id
	) a
left join
	(select
		a.sequence_id
		,condition.result a2624922
	from
		(select 
			seqid sequence_id
			,rules1.rule_id ruleid
			,rules1.conditions conditions
		from ods_tinyv_outer.univ_td_detail_v32_mongo_i
		lateral view explode(rawdata.rules) t as rules1
		where 
			dt <= date_add(current_date(),-1)
			and platformchannel = 'beike'
		) a
	lateral view explode(a.conditions) a as condition
	where a.ruleid = '2624922'
	) b
on a.risk_id = b.sequence_id
group by a.loan_id,a.risk_id,b.a2624922
