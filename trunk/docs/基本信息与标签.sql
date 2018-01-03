【用户基本信息语句】--> 对应sql文件名：user_info_new.sql
select
	a.loan_id loan_id
	,a.ds channel
	,c.user_gender gender
	,c.user_age age
	,b.mobile phone
	,c.mob_province attribution1_province
	,c.user_province_id	attribution2_province
	,case when b.mobile = b.reserve_mobile then 0 else 1 end is_phone_same
	,a.create_time
	,a.due_day
from
	(select
		loan_id
		,user_id
		,ds
		,create_time
		,min(due_day) due_day
	from edw_tinyv.e_repay_plan_detail_d
	where
		dt = date_add(current_date(),-1)
		and (to_date(due_day) <= date_add(current_date(),-1)
			 or
			 repay_status = '002005002')
	group by loan_id,user_id,ds,create_time
	) a
left join
	(select
		user_id
		,ds
		,mobile
		,reserve_mobile
	from ods_tinyv.o_fuse_user_info_contrast_d
	where
		dt = date_add(current_date(),-1)
	) b
on a.user_id = b.user_id and a.ds = b.ds
left join
	(select
		user_id
		,ds
		,user_gender
		,user_age
		,mobile_info['province'] mob_province
		,user_province_id
	from edw_tinyv.e_user_integ_info_d
	where 
		dt = date_add(current_date(),-1)
	) c
on a.user_id = c.user_id and a.ds = c.ds

【借款事件标签】--> 对应sql文件名：due_mask_info.sql
select
	loan_id
	,ds
	,max(case when early_flag = 1 then -early_day else overdue_day end) overdue_day
	,repay_status
from edw_tinyv.e_repay_plan_detail_d
where 
	dt = date_add(current_date(),-1)
	and (to_date(due_day) <= date_add(current_date(),-1)
	     or
	     repay_status = '002005002')
group by loan_id,ds,repay_status