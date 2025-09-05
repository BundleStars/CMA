--complete query

with pub_curr as (
	select 
	distinct order_date::date, currency, avg(local_price / nullif(revenue_inc_vat, 0)) conversion_to_publisher
	from shop.orders
	group by 1,2
	),
	
backup_pub_curr as (
    SELECT currency, avg(local_price / NULLIF(revenue_inc_vat,0)) conversion_to_publisher_backup
    from shop.orders
    where order_date::date = sysdate::date
    group by 1),
    
b2c_royalty as (
select 
	rod.supplier_name,
	nvl(isbn::text, sku)sku,
	rod.promo_name,
	rod.bundle_name,
	rod.product_name || NVL('\n in ' || NVL(rod.promo_name, rod.bundle_name), '') name,
	case when rod.bundle_name is not null then true else false end in_bundle,
	count(*) qty,
	ROUND(sum(revenue_ex_vat_pf * nvl(conversion_to_publisher,conversion_to_publisher_backup)) / 100,2) value,
	ROUND(sum((nvl(transaction_fee,0) / 100  + nvl(affiliate_commission,0) + nvl(affiliate_fee,0)) * nvl(conversion_to_publisher,conversion_to_publisher_backup)),2) deductions,
    ROUND(sum(revenue_ex_vat_pf * nvl(conversion_to_publisher,conversion_to_publisher_backup)) / 100 - sum((((nvl(transaction_fee, 0) / 100) + nvl(affiliate_commission,0) + nvl(affiliate_fee, 0)) * nvl(conversion_to_publisher,conversion_to_publisher_backup))),2) net,
    0.05 as "drivethru_percentage",
    ROUND(sum(exclusivity_fee * nvl(conversion_to_publisher, conversion_to_publisher_backup)) / 100,2) exclusivity_fee
from royalty.orders_details_2025_05_20250604113405 rod -- update this table
join shop.products p on p.product_id = rod.product_id 
left join pub_curr c on rod.order_date::date = c.order_date and 'USD' = c.currency
left join backup_pub_curr d on 'USD' = d.currency
where rod.drivethrurpg = true and rod.status in ('COMPLETE') 
and rod.order_date >= '2025-07-01' and rod.order_date < '2025-08-01' -- update this date range
group by 1,2,3,4,5,6
)

select 
	supplier_name,
	sku,
	"name",
	sum(qty) qty,
	sum(value) value,
	sum(deductions) deductions,
	sum(net) net,
	drivethru_percentage as "royalty_percentage",
	sum(exclusivity_fee) as "royalty",
	in_bundle
from b2c_royalty
group by 1,2,3,8,10, bundle_name, promo_name
order by supplier_name, NVL(bundle_name, ''), NVL(promo_name, ''), "name"
;


--refunds query
with pub_curr as (
    SELECT distinct order_date::date, currency, avg(local_price / NULLIF(revenue_inc_vat,0)) conversion_to_publisher 
    from shop.orders
    group by 1,2),
    
    backup_pub_curr as (
    SELECT currency, avg(local_price / NULLIF(revenue_inc_vat,0)) conversion_to_publisher_backup
    from shop.orders
    where order_date::date = sysdate::date
    group by 1
    ),

    partial_refund_dates as (
    SELECT order_id, min(date) first_partial_refund
    FROM shop.notes
    WHERE note like 'Partial refund of %'
    group by 1)
    
    select 
    od.supplier_name,
    nvl(isbn::text, p.sku) sku,
    product_name || NVL('\n in ' || NVL(promo_name, bundle_name), '') name,
    count(*) * -1 qty,
    ROUND(sum(revenue_ex_vat_pf * nvl(conversion_to_publisher,conversion_to_publisher_backup)) / 100,2) value,
	ROUND(sum((nvl(transaction_fee,0) / 100  + nvl(affiliate_commission,0) + nvl(affiliate_fee,0)) * nvl(conversion_to_publisher,conversion_to_publisher_backup)),2) * -1 deductions,
    ROUND(sum(revenue_ex_vat_pf * nvl(conversion_to_publisher,conversion_to_publisher_backup)) / 100 - sum((((nvl(transaction_fee, 0) / 100) + nvl(affiliate_commission,0) + nvl(affiliate_fee, 0)) * nvl(conversion_to_publisher,conversion_to_publisher_backup))),2) * -1 net,
    0.05 as "drivethru_percentage",
    ROUND(sum(exclusivity_fee * nvl(conversion_to_publisher, conversion_to_publisher_backup)) / 100,2) * -1 exclusivity_fee,
    case when bundle_name is not null then True else False end in_bundle
    from shop.order_details od
    join shop.products p on p.product_id = od.product_id
    left join partial_refund_dates prd on prd.order_id = od.order_id
    left join pub_curr c on od.order_date::date = od.order_date and 'USD' = c.currency
    left join backup_pub_curr d on 'USD' = d.currency
    where od.drivethrurpg = true 
    	and od.status != 'COMPLETE'
    	and od.order_date >= '2024-11-01'
    	and least(first_refund,first_fraud,first_chargeback,first_partial_refund) between '2025-07-01' and '2025-07-31' -- update this date range
    	and case when fixed_royalty_currency is not null then True when bundle_name is not null and od.royalty_percentage = 0 then False else True end
    group by 1,2,3,10,bundle_name,product_name,promo_name
    order by NVL(bundle_name,''), product_name
    ;