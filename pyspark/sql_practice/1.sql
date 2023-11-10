use sql_practice;
select sku_info.name, t2.total_sales
from sku_info join (
    select sku_id, rank() over (sort by order_sum desc) rk, price*order_sum total_sales
    from
        (
        select sku_id, price, sum(sku_num) order_sum
        from order_detail
        group by sku_id, price
        )
    ) t2 on t2.sku_id=sku_info.sku_id
    where t2.rk=2
;
