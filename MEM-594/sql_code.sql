declare date_from date default '2021-01-01';
declare date_to date default '2021-11-30';

with 
category_tree as (
    select 
    ca_id_leaf, 'Total' new_meta
    from 
    `sc-9369-dataengineering-prod.category_tree.budget_category_tree_actual_year`
    ),

offers as ( 
    select 
    FORMAT_DATE("%Y%m", date) year_month,
    seller_id,
    new_meta, 
    seller_segment,
    round(avg(offers),0) offers,
    min(date) min_date
    from (
        select 
        offers._PARTITIONDATE date,
        offers.seller_id, 
        case 
        when verified_company.account_id is not null and users.segment = 1 then "small" 
        when verified_company.account_id is not null and users.segment = 2 then "medium" 
        when verified_company.account_id is not null and users.segment = 3 then "large" 
        when verified_company.account_id is not null and users.segment = 4 then "VIP" 
        else null
        end as seller_segment,
        category_tree.new_meta,
        sum(offers.offers) offers
        from
        `sc-10024-com-analysts-prod.corporate_kpis.active_offers_daily` offers

        left join category_tree 
        on cast(category_tree.ca_id_leaf as integer) = offers.category_leaf
        
        left join `sc-10024-com-analysts-prod.monika_sekowska.company_verified_history` verified_company
        on verified_company.account_id = offers.seller_id
        and offers._PARTITIONDATE = verified_company._PARTITIONDATE
        and DATE(verified_company._PARTITIONTIME) between date_from and date_to

        left join `sc-9369-dataengineering-prod.dwh_import.tl_iz3_users` users
        on cast(users.us_id as string) = offers.seller_id
        and cast(FORMAT_DATE('%Y%m', offers._PARTITIONDATE) as int64) = users.month_

        where offers._PARTITIONDATE between date_from and date_to
        group by date, seller_id, new_meta, seller_segment
        ) 
    group by year_month, seller_id, new_meta, seller_segment
    ),

yesterday_offer as (
    select distinct 
    seller_id 
    from
    `sc-10024-com-analysts-prod.corporate_kpis.active_offers_daily` 
    where _PARTITIONDATE = current_date()-1 
    ),

orders as (
    SELECT 
    FORMAT_DATE("%Y%m", orders._PARTITIONDATE) year_month,
    orders.seller_id,
    category_tree.new_meta,
    sum(orders.price) GMV,
    count(orders.offer_id) count_deals
    FROM `sc-9369-dataengineering-prod.dwh.tl_gh9_order` orders

    left join category_tree
    on category_tree.ca_id_leaf = cast(orders.category_id as string)

    left join 
    `sc-9369-dataengineering-prod.dwh_import.dict_allegro_test_users` testowe
    on cast(testowe.us_id as string) = orders.seller_id

    WHERE 
    1=1 
    and DATE(orders._PARTITIONTIME) between date_from and date_to
    and testowe.us_id is null
    group by
    year_month,
    seller_id,
    new_meta
),

maincat as (
    select 
    seller_id, 
    new_meta,
    rank() over (partition by seller_id order by sum(GMV) desc, new_meta) as ranking,
    from orders
    group by 1,2

),

account_status as (
    with account_status as (
        SELECT
        cis_user_id,
        cis_status,
        cis_from_date,
        cis_historical
        FROM `sc-9369-dataengineering-prod.dwh_import.tl_ga6_company_icon_status` 
    )
    select 
    account_status.cis_user_id,
    account_status.cis_status,
    account_status.cis_from_date,
    case when coalesce(company_status.cis_user_id,0) = 0 then 0 else 1 end company
    from 
    account_status 
    left join 
    (select distinct cis_user_id from account_status
    where cis_status = 'company')  company_status
    on company_status.cis_user_id = account_status.cis_user_id
    where account_status.cis_historical = 'N'
    )

SELECT 
nip.nip,
offers.seller_id,
account_status.cis_status actual_account_status,
account_status.cis_from_date last_date_account_status,
account_status.company company_history,
offers.seller_segment,
offers.new_meta,
sum(orders.GMV) sum_GMV,
avg(orders.GMV) avg_GMV,
avg(orders.count_deals) avg_count_deals,
avg(offers.offers) avg_count_offers,
min(offers.min_date) min_date,
case when coalesce(yesterday_offer.seller_id,'0') = '0' then 0 else 1 end yesterday, 
count(distinct orders.year_month) count_month_sales,
count(distinct offers.year_month) count_month_offers
from
offers

left join orders
    on offers.seller_id = orders.seller_id 
    and orders.new_meta = offers.new_meta
    and orders.year_month = offers.year_month

left join account_status 
on cast(account_status.cis_user_id as string) = offers.seller_id

left join yesterday_offer
on yesterday_offer.seller_id = offers.seller_id

left join maincat
on maincat.seller_id = offers.seller_id
and maincat.ranking = 1

left join 
`sc-9369-dataengineering-prod.dwh_import.tl_iz3_updated_nip` nip
on cast(nip.ud_us_id as string) = offers.seller_id

#where offers.seller_id= '100010994'

group by 
nip,
seller_id,
actual_account_status,
last_date_account_status,
company_history,
offers.seller_segment,
offers.new_meta,
yesterday
order by 2