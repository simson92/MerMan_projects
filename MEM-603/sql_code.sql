declare date_from date default '2016-01-01';
declare date_to date default '2021-10-17';

select 
seller_id,
year,
verified,
verified - LAG(verified) OVER (
PARTITION BY seller_id 
ORDER BY seller_id, year) as verified_status,
sum_of_offers,
sum_of_offers_no_clasifieds
from 
(
select 
offers.seller_id,
offers.year,
offers.sum_of_offers,
offers.sum_of_offers_no_clasifieds,
case when COALESCE(verified_company.account_id,'0') = '0' then 0 else 1 end verified,
case when COALESCE(testowe.us_id,0) = 0 then 0 else 1 end konto_testowe
from 
(
SELECT
offers.seller_id,
extract(year from offers._PARTITIONDATE) year,
sum(offers.offers) sum_of_offers,
sum(offers.offers_no_classifieds) sum_of_offers_no_clasifieds
 FROM `sc-10024-com-analysts-prod.commercial.active_offers_daily_total` offers WHERE 
 1=1
and  _PARTITIONDATE between date_from  and date_to
group by offers.seller_id, year
) offers

left join 
(
    select 
    account_id,
    extract(year from _PARTITIONDATE) year
    from 
   `sc-10024-com-analysts-prod.monika_sekowska.company_verified_history` 
   where
   _PARTITIONDATE between date_from and date_to
   group by account_id, year
) verified_company
on verified_company.account_id = offers.seller_id
and offers.year = verified_company.year

left join 
`sc-9369-dataengineering-prod.dwh_import.dict_allegro_test_users` testowe
on cast(testowe.us_id as string) = offers.seller_id
)
where konto_testowe = 0
order by seller_id, year