-- 请向所有用户推荐其朋友收藏但是自己未收藏的商品，
-- 从好友关系表（friendship_info）和收藏表（favor_info）中查询出应向哪位用户推荐哪些商品。
-- 三种写法
    -- 本质上是union和EXCEPT的使用， EXCEPT等价于left join + is null
use sql_practice;
select friendship_info.user1_id user_id, favor_info.sku_id
from favor_info join friendship_info on favor_info.user_id=friendship_info.user2_id
group by friendship_info.user1_id, favor_info.sku_id
union -- friendship_info两列含义相同需要都处理 否则会漏
select friendship_info.user2_id user_id, favor_info.sku_id
from favor_info join friendship_info on favor_info.user_id=friendship_info.user1_id
group by friendship_info.user2_id, favor_info.sku_id
EXCEPT
select user_id, sku_id
from favor_info
group by user_id, sku_id
;

use sql_practice;
select t1.user_id, t1.sku_id from
(
    select friendship_info.user1_id user_id, favor_info.sku_id
    from favor_info join friendship_info on favor_info.user_id=friendship_info.user2_id
    group by friendship_info.user1_id, favor_info.sku_id
    union -- friendship_info两列含义相同需要都处理 否则会漏 也可直接先union user1_id
    select friendship_info.user2_id user_id, favor_info.sku_id
    from favor_info join friendship_info on favor_info.user_id=friendship_info.user1_id
    group by friendship_info.user2_id, favor_info.sku_id
) t1 left join favor_info t2  on  t1.user_id=t2.user_id and t1.sku_id=t2.sku_id
where t2.user_id is null


use sql_practice;
select
    distinct t1.user_id,
    friend_favor.sku_id
from
(
    select
        user1_id user_id,
        user2_id friend_id
    from friendship_info
    union -- 第一个表指定列名 第二个表指定位置匹配
    select
        user2_id,
        user1_id
    from friendship_info
)t1
join favor_info friend_favor
on t1.friend_id=friend_favor.user_id
left join favor_info user_favor
on t1.user_id=user_favor.user_id
and friend_favor.sku_id=user_favor.sku_id
where user_favor.sku_id is null;
