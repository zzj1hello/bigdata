
-- 从订单明细表（order_detail）中筛选出2021年总销量小于100的商品及其销量，并且不考虑上架时间小于一个月的商品
-- 假设今天的日期是2022-01-10，
use sql_practice;
select t2.sku_id, name, order_sum
from sku_info join
(
    select sku_id, sum(sku_num) order_sum
    from order_detail
    where year(create_date)=='2021' and date_diff('2022-01-10', create_date) >= 30
    group by sku_id
    having sum(sku_num)<100
) t2 on sku_info.sku_id=t2.sku_id
;