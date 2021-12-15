declare date_start date default '2020-12-01';
declare date_stop date default current_date()-1;
declare last_day_of_month date default date_trunc(current_date(), month)-1;
declare start_of_3_month_period date default date_trunc(last_day_of_month-65, month);

with 

last_3_month_period as(
    SELECT     
    FORMAT_DATE("%Y%m", date) month,
    FROM UNNEST(GENERATE_DATE_ARRAY(start_of_3_month_period, last_day_of_month, INTERVAL 1 MONTH)) AS date
),

user_data as (
    select 
    ud_nip,
    ud_us_id 
    from 
    `sc-9369-dataengineering-prod.dwh_import.tl_ga6_users_data`
    where ud_nip is not null
    ),

category_tree as (
    select 
    ca_id_leaf, 
    new_meta,
    new_ca_name_1
    from 
    `sc-9369-dataengineering-prod.category_tree.budget_category_tree_actual_year`
    ),

# w orders i refunds można dodawać kolejne kolumny do agregacji - trzeba tylko pamiętać dodać w gmv_monthly kolumnę oraz klucz 
orders as (
    select   
    user_data.ud_nip nip,
    FORMAT_DATE("%Y%m", orders._PARTITIONDATE) month,
    category_tree.new_meta,
    category_tree.new_ca_name_1,
    case buyers.account_type when 'company' then 'B2B' else 'B2C' end order_type,
    sum(orders.price) gmv
    from `sc-9369-dataengineering-prod.dwh.tl_gh9_order` orders
    join user_data
    on orders.seller_id = cast(user_data.ud_us_id as string)
    left join `sc-9369-dataengineering-prod.dwh.tl_gi4_users_history` buyers
    on buyers.account_id = cast(orders.buyer_id as int)
    and buyers._PARTITIONDATE = orders._PARTITIONDATE
    left join category_tree
    on cast(category_tree.ca_id_leaf as int) = orders.category_id
    where 
    1=1 
    and orders._partitiondate BETWEEN date_start AND date_stop
    group by 1,2,3,4,5        
    ),

refunds as (
    select 
    user_data.ud_nip nip,
    FORMAT_DATE("%Y%m", refunds._PARTITIONDATE) month,
    category_tree.new_meta, 
    category_tree.new_ca_name_1,
    case buyers.account_type when 'company' then 'B2B' else 'B2C' end order_type,
    sum(refunds.refunded_gmv) ref_gmv
    from 
    `sc-9369-dataengineering-prod.orders.refunds` refunds 
    join user_data 
    on user_data.ud_us_id = refunds.seller_id
    left join `sc-9369-dataengineering-prod.dwh.tl_gh9_order` orders 
    on orders.order_offer_id = refunds.order_offer_id
    and orders._PARTITIONDATE between date_start and date_stop 
    left join `sc-9369-dataengineering-prod.dwh.tl_gi4_users_history` buyers
    on buyers.account_id = cast(orders.buyer_id as int)
    and buyers._PARTITIONDATE = orders._PARTITIONDATE
    left join category_tree
    on cast(category_tree.ca_id_leaf as int) = orders.category_id
    where
    1=1 
    and refunds._PARTITIONDATE between date_start and date_stop
    group by 1,2,3,4,5
    ),
    
gmv_monthly as (
    select 
    orders.nip, 
    orders.month,
    coalesce(orders.order_type, refunds.order_type) order_type,
    coalesce(orders.new_meta, refunds.new_meta) new_meta,
    coalesce(orders.new_ca_name_1, refunds.new_ca_name_1) new_ca_name_1,
    sum(orders.gmv) - sum(coalesce(refunds.ref_gmv,0)) gmv
    from 
    orders
    full outer join refunds
    on refunds.nip = orders.nip 
    and refunds.month = orders.month 
    and refunds.new_meta = orders.new_meta
    and refunds.new_ca_name_1 = orders.new_ca_name_1
    and refunds.order_type = orders.order_type
    group by 1,2,3,4,5
),

gmv_annual as (
    select 
    nip,
    sum(gmv) gmv
    from gmv_monthly 
    where 
    1=1
    and gmv_monthly.month <> format_date("%Y%m",date_stop)
    group by 1
    ),   

gmv_3_last_month_order_type as (
    select 
    *
    from (
        select
        nip, 
        order_type, 
        round(sum(gmv),2) gmv 
        from gmv_monthly 
        join last_3_month_period t
        on t.month = gmv_monthly.month
        group by 1,2
        )
        PIVOT 
        (sum(gmv) as gmv
        for order_type in ('B2B', 'B2C')
        )
    ),

gmv_monthly_pivot as ( 
    select 
    * 
    from (
        select nip, month, round(sum(gmv),2) gmv from gmv_monthly group by 1,2
        )
        PIVOT 
        ( sum(gmv) as gmv
        for month in ('202012','202101','202102','202103','202104','202105','202106','202107','202108','202109','202110','202111','202112')
        )
    ),   

gmv_order_type_pivot as (
    select 
    *
    from (
        select
        nip, 
        order_type, 
        round(sum(gmv),2) gmv
        from gmv_monthly 
        where 
        1=1
        #and gmv_monthly.month <> format_date("%Y%m",date_stop) 
        group by 1,2
        )
        PIVOT 
        (sum(gmv) as gmv
        for order_type in ('B2B', 'B2C')
        )
    ),

user as (
    select 
    nip
    from (
        SELECT 
        ud_nip nip,
        case when recommend is true then 1 else 0 end as pol,
        recommend,
        create_date
        FROM `sc-9369-dataengineering-prod.dwh.tl_gr2_ratings` r
        join `sc-10024-com-analysts-prod.monika_sekowska.company_verified_history` c
        on c.account_id = r.opponent_id 
        and c._partitiondate BETWEEN date_start AND date_stop 
        join user_data 
        on r.opponent_id = cast(user_data.ud_us_id as string)
        WHERE r._PARTITIONDATE = date_sub(current_date, interval 1 day)
        AND date(CREATE_DATE) BETWEEN date_start AND date_stop ) a
    group by nip
    having sum(pol)/count(recommend) >= 0.98
    order by 1
    ),
      
nipy as (
    select 
    gmv_monthly.nip 
    from (
        select 
        nip,
        month,
        sum(gmv) gmv,
        case when sum(gmv) >= 100000 then 1 else 0 end licznik
        from gmv_monthly
        group by 1,2
        ) gmv_monthly
    join user u
    on u.nip = gmv_monthly.nip
    group by gmv_monthly.nip    
    having sum(licznik) >= 6
    ),

jakosc as (  
    select 
    ud_nip,
    sum(pol)
      , count(recommend)
      , sum(pol)/count(recommend) polecam
    from(
        SELECT 
        ud_nip,
        case when recommend is true then 1 else 0 end as pol,
        recommend,
        create_date
        FROM `sc-9369-dataengineering-prod.dwh.tl_gr2_ratings` r
        join user_data
        on r.opponent_id = cast(user_data.ud_us_id as string)
        WHERE 
        1=1
        AND r._PARTITIONDATE = date_sub(current_date, interval 1 day)
        AND date(CREATE_DATE) BETWEEN date_start AND date_stop
        ) a
    group by ud_nip
    order by 1
    ),

jakosc_3m as (    
    select 
    ud_nip,
    sum(pol),
    count(recommend),
    sum(pol)/count(recommend) polecam
    from(
        SELECT 
        ud_nip,
        case when recommend is true then 1 else 0 end as pol,
        recommend,
        create_date
        FROM `sc-9369-dataengineering-prod.dwh.tl_gr2_ratings` r
        join user_data
        on r.opponent_id = cast(user_data.ud_us_id as string)
        WHERE 
        1=1
        and r._PARTITIONDATE = date_sub(current_date, interval 1 day)
        AND date(CREATE_DATE) BETWEEN start_of_3_month_period AND date_stop
        ) a
    group by ud_nip
    order by 1
    ),

maincat as(
    select 
    gmv_monthly.nip, 
    gmv_monthly.new_meta, 
    sum(gmv_monthly.gmv) gmv_main, 
    rank() over (partition by gmv_monthly.nip order by sum(gmv_monthly.gmv) desc, gmv_monthly.new_meta) as ranking
    from gmv_monthly 
    where 
    1=1 
    and gmv_monthly.month <> format_date("%Y%m",date_stop)
    group by 1,2
    ), 

maincat1 as(
    select 
    gmv_monthly.nip, 
    gmv_monthly.new_meta, 
    gmv_monthly.new_ca_name_1,
    sum(gmv_monthly.gmv) gmv_main1, 
    rank() over (partition by gmv_monthly.nip order by sum(gmv_monthly.gmv) desc, gmv_monthly.new_meta) as ranking
    from gmv_monthly 
    where 
    1=1 
    and gmv_monthly.month <> format_date("%Y%m",date_stop)
    group by 1,2,3
    )
 
select 
n.*, 
round(jakosc.polecam,4) polecam, 
round(j3.polecam,4) jakosc_3m, 
round(gmv_annual.gmv,2) gmv12m, 
m.new_meta, 
round(m.gmv_main,2) gmv, 
m1.new_ca_name_1, 
round(m1.gmv_main1,2) gmv1,
-- m2.new_ca_name_2, m2.gmv as gmv2
m_n.new_meta second_new_meta, 
round(m_n.gmv_main,2) second_new_meta_gmv,
gmv_monthly_pivot.* except (nip),
gmv_order_type_pivot.* except (nip),
gmv_order_type.* except (nip)
from nipy n
join jakosc 
on n.nip =jakosc.ud_nip 
join maincat m
on m.nip = n.nip 
and ranking = 1
join maincat1 m1
on m1.nip = n.nip 
and m1.ranking = 1
left join gmv_annual
on gmv_annual.nip = n.nip 
left join jakosc_3m j3
on j3.ud_nip = n.nip
left join  maincat m_n
on m_n.nip = n.nip 
and m_n.ranking = 2
left join gmv_monthly_pivot  
on gmv_monthly_pivot.nip = n.nip
left join gmv_order_type_pivot
on gmv_order_type_pivot.nip = n.nip
left join gmv_3_last_month_order_type gmv_order_type
on gmv_order_type.nip = n.nip
--where n.nip = '5931730789'
