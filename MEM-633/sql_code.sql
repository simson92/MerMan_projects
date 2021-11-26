select 
nip, 
segment,
segment_desc, 
case when allegro_marketing > 0 then 1 else 0 end allegro_marketing,
case when offers > 0 then 1 else 0 end activity_0111
from 
(
select 
nip, 
segment, 
case segment when 1 then "small" when 2 then "medium" when 3 then "large" when 4 then "VIP" end as segment_desc,
sum(allegro_marketing) allegro_marketing,
sum(offers) offers
from 
(
SELECT 
verified.account_id,
nip.nip ,
users_segment.segment, 

#users_segment.classified,
case when COALESCE(allegro_marketing.userid,'False') = 'False' then 0 else 1 end allegro_marketing,
case when COALESCE(testowe.us_id,0) = 0 then 0 else 1 end konto_testowe,
coalesce(offers,0) offers
FROM 
`sc-10024-com-analysts-prod.monika_sekowska.company_verified_history` verified

left join 
`sc-9369-dataengineering-prod.dwh_import.tl_iz3_updated_nip` nip
on cast(nip.ud_us_id as string) = verified.account_id

left join 
`sc-9369-dataengineering-prod.dwh_import.tl_iz3_users` users_segment
on cast(users_segment.us_id as string) = verified.account_id
and cast(users_segment.month_ as string) = '202111'

left join 
`sc-9369-dataengineering-prod.dwh_import.dict_allegro_test_users` testowe
on cast(testowe.us_id as string) = verified.account_id 

left join 
(
select userid
from `sc-11686-communication-prod.communication.user_consents_state_v2`
where 
1=1 
and _partitiondate = '2021-11-01'
and consentname = 'allegro-marketing'
and approved = true
group by userid
)
allegro_marketing
on allegro_marketing.userid = verified.account_id

left join 
(
select seller_id, sum(offers) offers
from `sc-10024-com-analysts-prod.commercial.active_offers_daily_total` 
where _PARTITIONDATE = current_date()-1
group by seller_id
) offers_yesterday
on offers_yesterday.seller_id = verified.account_id 

WHERE _PARTITIONDATE = "2021-11-01"
#and nip.nip = '186864'

)
where konto_testowe = 0
group by nip, 
segment
)