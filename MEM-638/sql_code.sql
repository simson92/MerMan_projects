select 
nip,
oferta_od_2016,
max(success_login_timestamp) max_log,
count(account_id)
from 
(
select 
verified.account_id,
nip.nip,
case when COALESCE(testowe.us_id,0) = 0 then 0 else 1 end konto_testowe,
case when coalesce(offers.seller_id,'0') = '0' then 0 else 1 end oferta_od_2016,
last_log.success_login_timestamp
from `sc-10024-com-analysts-prod.monika_sekowska.company_verified_history` verified

left join 
`sc-9369-dataengineering-prod.dwh_import.tl_iz3_updated_nip` nip
on cast(nip.ud_us_id as string) = verified.account_id

left join 
`sc-9369-dataengineering-prod.dwh_import.dict_allegro_test_users` testowe
on cast(testowe.us_id as string) = verified.account_id 

left join 
`sc-9369-dataengineering-prod.dwh.tl_gi4_users` last_log
on last_log.account_id = cast(verified.account_id as integer)
and last_log._PARTITIONDATE = current_date()-1

left join 
(select distinct seller_id from 
    `sc-10024-com-analysts-prod.commercial.active_offers_daily_total`) offers
on offers.seller_id = verified.account_id

where 
verified._PARTITIONDATE = current_date()-1

) 
where konto_testowe = 0
group by nip, oferta_od_2016