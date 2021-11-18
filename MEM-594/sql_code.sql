declare date_from date default '2021-01-01';
declare date_to date default '2021-10-31';

with 
category_tree as (
    select 
    ca_id_leaf, new_meta
    from 
    `sc-9369-dataengineering-prod.category_tree.budget_category_tree_actual_year`
    ),

offers as ( 
    select 
    extract(year from date) || extract(month from date) year_month,
    seller_id,
    new_meta, 
    avg(offers) offers
    from(
    select 
    _PARTITIONDATE date,
    #extract(year from _PARTITIONDATE) || extract(month from _PARTITIONDATE) year_month,
    offers.seller_id, 
    category_tree.new_meta,
    sum(offers.offers) offers
    from
    `sc-10024-com-analysts-prod.corporate_kpis.active_offers_daily` offers
    left join `sc-9369-dataengineering-prod.category_tree.budget_category_tree_actual_year` category_tree
    on cast(category_tree.ca_id_leaf as integer) = offers.category_leaf
    where _PARTITIONDATE between date_from and date_to
    group by date, seller_id, new_meta  
    ) 
    group by year_month, seller_id, new_meta),

yesterday_offer as (
    select distinct 
    seller_id 
    from
    `sc-10024-com-analysts-prod.corporate_kpis.active_offers_daily` 
    where _PARTITIONDATE = current_date()-1 
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
zgrupowane.nip,
zgrupowane.seller_id,
account_status.cis_status actual_account_status,
account_status.cis_from_date last_date_account_status,
account_status.company company_history,
zgrupowane.konto_testowe,
zgrupowane.verified,
zgrupowane.seller_segment,
zgrupowane.new_meta,
avg(zgrupowane.GMV) avg_GMV,
avg(zgrupowane.count_deals) avg_count_deals,
avg(offers.offers) avg_count_offers,
case when coalesce(yesterday_offer.seller_id,'0') = '0' then 0 else 1 end yesterday, 
count(distinct zgrupowane.year_month) count_month_sales,
count(distinct offers.year_month) cont_month_offers
from
    (
    SELECT 
    extract(year from orders._PARTITIONDATE) || extract(month from orders._PARTITIONDATE) year_month,
    nip.nip,
    orders.seller_id,
    case when verified_company.account_id is null then 0 else 1 end verified,
    case 
    when verified_company.account_id is not null and users.segment = 1 then "small" 
    when verified_company.account_id is not null and users.segment = 2 then "medium" 
    when verified_company.account_id is not null and users.segment = 3 then "large" 
    when verified_company.account_id is not null and users.segment = 4 then "VIP" 
    else null
    end as seller_segment,
    category_tree.new_meta,
    case when testowe.us_id is null then 0 else 1 end konto_testowe,
    sum(orders.price) GMV,
    count(orders.offer_id) count_deals
    FROM `sc-9369-dataengineering-prod.dwh.tl_gh9_order` orders

    left join category_tree
    on category_tree.ca_id_leaf = cast(orders.category_id as string)

    left join `sc-9369-dataengineering-prod.dwh_import.tl_iz3_users` users
    on cast(users.us_id as string) = orders.seller_id
    and cast(FORMAT_DATE('%Y%m', orders._PARTITIONDATE) as int64) = users.month_

    left join `sc-10024-com-analysts-prod.monika_sekowska.company_verified_history` verified_company
    on verified_company.account_id = orders.seller_id
    and orders._PARTITIONDATE = verified_company._PARTITIONDATE
    and DATE(verified_company._PARTITIONTIME) between date_from and date_to

    left join 
    `sc-9369-dataengineering-prod.dwh_import.tl_iz3_updated_nip` nip
    on cast(nip.ud_us_id as string) = orders.seller_id

    left join 
    `sc-9369-dataengineering-prod.dwh_import.dict_allegro_test_users` testowe
    on cast(testowe.us_id as string) = orders.seller_id

    WHERE 
    1=1 
    and DATE(orders._PARTITIONTIME) between date_from and date_to
    group by
    year_month,
    nip,
    seller_id,
    verified,
    seller_segment,
    new_meta,
    konto_testowe
    ) zgrupowane 

left join offers 
    on offers.seller_id = zgrupowane.seller_id 
    and zgrupowane.new_meta = offers.new_meta
    and zgrupowane.year_month = offers.year_month


left join account_status 
on cast(account_status.cis_user_id as string) = zgrupowane.seller_id

left join yesterday_offer
on yesterday_offer.seller_id = zgrupowane.seller_id

group by 
zgrupowane.nip,
zgrupowane.seller_id,
actual_account_status,
last_date_account_status,
company_history,
zgrupowane.verified,
zgrupowane.seller_segment,
zgrupowane.new_meta,
zgrupowane.konto_testowe,
yesterday