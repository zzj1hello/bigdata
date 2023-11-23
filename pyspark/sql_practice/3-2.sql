select distinct user_id
from(

      select user_id
           , create_date
            -- 关键的flag计算：使用date_sub(日期, 减去的差值) 得到的flag相同表示连续 再按照flag分组判断个数是否满足条件即可
           , date_sub(create_date, row_number() over (partition by user_id order by create_date)) flag
      from (
               select user_id
                    , create_date
               from sql_practice.order_info
               -- groupby会根据返回的列自动去重（相同分组号）因此后续使用三种不同的rank都一样
               group by user_id, create_date
           )

    )
group by user_id, flag
having count(flag) >= 3
