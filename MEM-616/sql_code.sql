with tab_offers as (
SELECT _PARTITIONDATE date, 
seller_id, 
offers
FROM `sc-10024-com-analysts-prod.commercial.active_offers_daily_total` 
WHERE _PARTITIONDATE = current_date()-1
)

SELECT
nip.ud_nip,
sum(offers) offers
from tab_offers
inner join `sc-10024-com-analysts-prod.commercial.updated_nip_full` nip
on cast(nip.ud_us_id as string) = tab_offers.seller_id

inner join `sc-10024-com-analysts-prod.szymon_plachta.MEM-616_nipy` nipy_michal
on cast(nip.ud_nip as string) = nipy_michal.nip

group by ud_nip