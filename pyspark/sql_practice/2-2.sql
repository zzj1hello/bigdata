
-- 查询每日新增用户数
use sql_practice;

select login_date_first, count(distinct user_id) user_count
from
(
    select user_id, min(date_format(login_ts, "yyyy-MM-dd")) login_date_first
    from user_login_detail
    group by user_id

) t2
group by t2.login_date_first
;
