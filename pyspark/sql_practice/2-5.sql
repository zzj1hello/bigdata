-- 从订单信息表（order_info）和用户信息表（user_info）中，
-- 分别统计每天男性和女性用户的订单总金额，如果当天男性或者女性没有购物，则统计结果为0。

use sql_practice;
select t1.create_date, sum(if(t2.gender=='男', t1.total_amount, 0)) total_amount_male
    , sum(if(t2.gender=='女', t1.total_amount, 0)) total_amount_female
from order_info t1 join user_info t2 on t1.user_id=t2.user_id
group by t1.create_date
;


select create_date,
  cast(sum(if(gender = '男', total_amount, 0)) as decimal(16, 2)) total_amount_male,
  cast(sum(if(gender = '女', total_amount, 0)) as decimal(16, 2)) total_amount_female from order_info oi
         join
     user_info ui
     on oi.user_id = ui.user_id
group by create_date;
