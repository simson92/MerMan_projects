with
tab_offers as (
    SELECT distinct
    seller_id
    FROM `sc-10024-com-analysts-prod.commercial.active_offers_daily_total` 
    WHERE _PARTITIONDATE = current_date()-1
), 
tab_GMV as (
    select 
    account_id,
    min(dt) min_date,
    count(distinct dt) days_with_sales,
    sum(GMV_ref) sum_gmv,
    avg(GMV_ref) avg_gmv    
    from `sc-10024-com-analysts-prod.commercial.GMV_refunds_category`
    where dt >= '2020-09-30'
    group by account_id
    ),

loginy as (
    SELECT distinct id, konto_allegro
    FROM `sc-16611-cortex-prod.szymon_plachta.MEM-626` 
    ),

webinary as (
    SELECT id, count(*) liczba, string_agg(webinar_tytul, ', '), min(webinar_tytul.) webinar_tytul
    FROM `sc-16611-cortex-prod.szymon_plachta.MEM-626` 
    group by  id
    )


SELECT
users.login login, 
users.account_id id,
nip.ud_nip nip,
loginy.konto_allegro,
users.account_create_date,
tab_GMV.min_date first_sale_date,
tab_GMV.days_with_sales,
round(tab_GMV.sum_gmv,2) sum_GMV_ref,
round(tab_GMV.avg_gmv,2) daily_avg_GMV,
case coalesce(tab_offers.seller_id,'brak') when 'brak' then 'no' else 'yes' end as have_offer_yesterday,
webinary.webinar_tytul,
webinary.liczba
FROM `sc-9369-dataengineering-prod.dwh.tl_gi4_users` users

left join `sc-10024-com-analysts-prod.commercial.updated_nip_full` nip
on nip.ud_us_id = users.account_id

RIGHT join loginy on loginy.id = users.account_id

# GMV 
left join tab_GMV 
on cast(tab_GMV.account_id as integer) = users.account_id

left join tab_offers 
on cast(tab_offers.seller_id as integer) = users.account_id

left join webinary 
on webinary.id = users.account_id
where
1=1
and users._PARTITIONDATE = current_date() - 1
