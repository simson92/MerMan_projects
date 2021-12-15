declare v_start_date date default '2021-09-01';
declare v_stop_date date default '2021-11-30';

with 
vips as (
    select 
    vip.nip,
    user_data.ud_us_id id 
    from 
    `sc-16611-cortex-prod.szymon_plachta.MEM-712_nipy` vip
    left join 
    `sc-9369-dataengineering-prod.dwh_import.tl_ga6_users_data` user_data
    on user_data.ud_nip = vip.nip 
    ),

billings as (
    select
    account_id id, 
    short_type, 
    billing_desc.my_group_desc,
    gross_amount,
    FORMAT_DATE("%Y%m", _PARTITIONDATE) month
    from `sc-9369-dataengineering-prod.billings.billings` billings
    join 
    `sc-16611-cortex-prod.szymon_plachta.MEM-712_billing_types` billing_desc
    on billings.short_type = billing_desc.id
    and my_group_desc is not null
    where 
    1=1 
    and billings._PARTITIONDATE between v_start_date and v_stop_date
    and billings.marketplace = 'allegro.pl'
    )

select 
* 
from (
    select 
    billings.month,
    vips.nip,
    billings.short_type,
    billings.my_group_desc,
    sum(billings.gross_amount) amount
    from 
    billings
    join vips 
    on vips.id = cast(billings.id as int)
    where 
    1=1
#    and vips.nip = '9492191268'
    group by 1,2,3,4 
)
where abs(amount) > 0
