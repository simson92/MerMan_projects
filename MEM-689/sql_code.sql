DECLARE date_from date default '2021-10-01';
DECLARE date_to date default '2021-10-31';

with nip as (
    select * from `sc-10024-com-analysts-prod.commercial.updated_nip_full` 
    where ud_nip = '5222722127'
    )

SELECT 
'refund' typ,
nip.ud_nip, 
refund.order_id,
refund.offer_id, 
refund.refund_quantity, 
refund.refunded_gmv,
date(refund.order_buying_date) order_date,
date(refund.refund_finish_date) refund_date
FROM 
`sc-9369-dataengineering-prod.orders.refunds` refund 
inner join nip
on nip.ud_us_id = refund.seller_id
WHERE 
1=1 
and _PARTITIONDATE between date_from and date_to 
--and refund.order_buying_date between '2021-10-01' and '2021-10-31'

union all 

select 
'order' typ,
nip.ud_nip,
orders.deal_id,
cast(orders.offer_id as integer),
orders.quantity,
orders.price,
DATE(orders.buyingtime) date,
null date_2
from `sc-9369-dataengineering-prod.dwh.tl_gh9_order` orders
inner join nip 
on cast(nip.ud_us_id as string) = orders.seller_id
WHERE 
_PARTITIONDATE between date_from and date_to