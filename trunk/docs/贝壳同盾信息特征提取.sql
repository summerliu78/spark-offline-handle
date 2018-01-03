【贝壳同盾风险、信用分】【贝壳渠道与其他三家不一样】
set hive.auto.convert.join=false;
select
	a.loan_id loan_id	--借款id
	,'beike' channel	--借款时间渠道
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
		`_id` risk_id
		,final_score
		,credit_score['id_number_mobile_score'] credit_score
	from ods_tinyv_outer.univ_td_mongo_i
	where dt <= date_add(current_date(),-1)
	) b
on a.risk_id = b.risk_id
group by a.loan_id,b.final_score,b.credit_score

------------------------------------------------
【贝壳同盾风险、信用分】【贝壳渠道与其他三家不一样】
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
		td.sequence_id,
		sum(case when td.ruleid = '2624922' then td.cnt  else '' end) a2624922
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
					seqid sequence_id
					,rules1.rule_id ruleid
					,rules1.conditions conditions
				from ods_tinyv_outer.univ_td_detail_v32_mongo_i
				lateral view explode(rawdata.rules) t as rules1
				where dt <= date_add(current_date(),-1)
				) a
			lateral view explode(a.conditions) a as condition
			) b
		lateral view explode(hits) b as hit
		)td
	group by td.sequence_id
	) b
on a.risk_id = b.sequence_id
group by a.loan_id,a.risk_id,b.a2624922