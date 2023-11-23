
-- 从用户登录明细表（user_login_detail）和订单信息表（order_info）中
-- 查询每个用户的注册日期（首次登录日期）、总登录次数，以及2021年的登录次数、订单数和订单总额。
use sql_practice;

select t2.user_id, t2.register_date, t2.total_login_count,
       -- nvl(val, default_val) 左连接 保证没有下单的用户下单数和金额值为0
       nvl(t3.order_count_2021, 0),
       cast(nvl(t3.order_amount_2021, 0) as decimal(16, 2))
from
(
    select user_id, min(date_format(login_ts, "yyyy-MM-dd")) register_date, count(*) total_login_count,
           -- count(if()) 进行带条件的聚合
           count(if(year(login_ts)=='2021', 1, 0)) login_count_2021
    from user_login_detail
    group by user_id

) t2 left join (
    select user_id, count(*) order_count_2021, sum(total_amount) order_amount_2021
    from order_info
    where year(create_date)='2021'
    group by user_id
) t3 on t2.user_id=t3.user_id

;
