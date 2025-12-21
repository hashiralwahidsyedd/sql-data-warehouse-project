/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

if object_id('gold.dim_customers','v') is not null
	drop view gold.dim_customers;

go 

create view gold.dim_customers as

select 
	row_number() over (order by ci.cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	el.cntry as country,
	case 
		when ci.cst_gndr = 'n/a' then coalesce(ec.gen,'n/a')
		else ci.cst_gndr
	end as gender,
	ci.cst_marital_status as martial_status,
	ec.bdate as birthdate,
	ci.cst_create_date as create_date

from silver.crm_cust_info ci
left join silver.erp_cust_az12 ec
on ci.cst_key = ec.cid
left join silver.erp_loc_a101 el
on ci.cst_key = el.cid

go
-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================


if object_id('gold.dim_products','v') is not null
	drop view gold.dim_products;

go 

create view gold.dim_products as 
select 
	row_number() over(order by cp.prd_start_dt,cp.prd_key) as product_key, 
	cp.prd_id as product_id,
	cp.prd_key as product_number,
	cp.prd_nm as product_name,
	cp.cat_id as category_id,
	ep.cat as category,
	ep.subcat as subcategory,
	ep.maintenance as maintenance_required,
	cp.prd_cost as cost,
	cp.prd_line as product_line,
	cp.prd_start_dt as start_date

from silver.crm_prd_info cp
left join silver.erp_px_cat_g1v2 ep
on cp.cat_id = ep.id
where cp.prd_end_dt is null

go

-- =============================================================================
-- Create Dimension: gold.fact_sales
-- =============================================================================

if object_id('gold.fact_sales','v') is not null
	drop view gold.fact_sales;

go 
create view gold.fact_sales as
select 
	cd.sls_ord_num as order_number,
	dc.customer_key as customer_key,
	dp.product_key as product_key,
	cd.sls_order_dt as order_date,
    cd.sls_ship_dt as shipping_date,
	cd.sls_due_dt as due_date,
	cd.sls_sales as sales_amount,
	cd.sls_quantity as quantity,
	cd.sls_price as price

from silver.crm_sales_details cd
left join gold.dim_customers dc
on cd.sls_cust_id = dc.customer_id
left join gold.dim_products dp
on cd.sls_prd_key = dp.product_number

go
