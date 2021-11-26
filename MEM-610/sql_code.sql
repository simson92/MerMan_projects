with
lista_kont as(
select * from `sc-10024-com-analysts-prod.jakub_wasag.MEM_553_lista_kont`
)

,zgody as (
select 
userid,	
consentname,
channel	
from `sc-11686-communication-prod.communication.user_consents_state_v2`
where 
1=1 
and _partitiondate = date_sub (current_date,interval 1 day)
and approved = true
and consentname in ('allegro-marketing', 'invitations-to-testing')
and channel in ('PHONE','EMAIL')
)

, baza as(
select l. *, login, account_status, account_type 
from lista_kont l
left join `sc-9369-dataengineering-prod.dwh.tl_gi4_users` u on us_id = account_id and u._partitiondate = current_date-1
)

, gmv as (
select us_id, sum(GMV_ref) GMV_30dni
from `sc-10024-com-analysts-prod.commercial.GMV_refunds_category` g 
inner join baza b on g.account_id = cast(b.us_id as string)
where dt between date_sub(current_date-1, interval 29 day) and current_date-1 
group by 1
)

select
b.*, 
gmv_30dni, 
offers, 
case when zgody_marketing.userid is not null then 'tak' else 'nie' end zgoda_marketing_tel, 
case when zgody_testing.userid is not null then 'tak' else 'nie' end zgoda_testing_mail,
from baza b
left join gmv g on b.us_id = g.us_id
left join `sc-10024-com-analysts-prod.commercial.active_offers_daily_total` o on cast(g.us_id as string) = seller_id and o._partitiondate = current_date-1
left join 
(select distinct userid from zgody where consentname = 'allegro-marketing' and channel in ('PHONE')) zgody_marketing on zgody_marketing.userid = cast(b.us_id as string)
left join
(select distinct userid from zgody where consentname = 'invitations-to-testing' and channel in ('EMAIL')) zgody_testing on zgody_testing.userid = cast(b.us_id as string)

order by 2,1