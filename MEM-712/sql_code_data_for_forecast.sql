declare date_start date default '2017-07-01';
declare date_stop date default '2021-11-30';

with 

dates as(
    SELECT     
    FORMAT_DATE("%Y%m", date) month,
    FROM UNNEST(GENERATE_DATE_ARRAY(date_start, date_stop, INTERVAL 1 MONTH)) AS date
),

vips as (
    select 
    vip.nip,
    user_data.ud_us_id id 
    from 
    `sc-16611-cortex-prod.szymon_plachta.MEM-712_nipy` vip
    left join 
    `sc-9369-dataengineering-prod.dwh_import.tl_ga6_users_data` user_data
    on user_data.ud_nip = vip.nip 
    and ud_nip is not null
),

vips_temp as (
    select 
    vip.nip,
    dates.month
    from 
    `sc-16611-cortex-prod.szymon_plachta.MEM-712_nipy` vip
    cross join dates 
),

offers as ( 
    select 
    FORMAT_DATE("%Y%m",date) month,
    nip,
    round(avg(offers),2) offers
    from (
        select 
        offers._PARTITIONDATE date, 
        vips.nip, 
        sum(offers.offers) offers
        from
        `sc-10024-com-analysts-prod.commercial.active_offers_daily_total` offers
        join vips 
        on vips.id = cast(offers.seller_id as int)
        where _PARTITIONDATE between date_start and date_stop
        group by 1,2
        ) 
        group by 1,2

),

gmv as (
    select 
    FORMAT_DATE('%Y%m', gmv.dt) month,
    vips.nip,
    sum(gmv.GMV_ref) gmv
    from 
    `sc-10024-com-analysts-prod.commercial.GMV_refunds_category` gmv 
    right join vips 
    on vips.id = cast(gmv.account_id as int)
    where dt between date_start and date_stop 
    group by 1,2
)

select 
vips_temp.nip,
vips_temp.month,
coalesce(offers.offers,0) offers,
coalesce(gmv.gmv,0) gmv
from 
vips_temp 
left join
offers  
on offers.month  = vips_temp.month 
and offers.nip = vips_temp.nip
left join gmv 
on gmv.nip = offers.nip 
and gmv.month = offers.month
where 
1=1 
order by 
1,2

