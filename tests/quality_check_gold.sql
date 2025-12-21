
/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results 
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.product_key'
-- ====================================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================
-- Check the data model connectivity between fact and dimensions
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL  




---personal checks and filter during process

--check duplicate after joining

select cst_id, count(*) from

(select 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ec.bdate,
	ec.gen,
	el.cntry

from silver.crm_cust_info ci
left join silver.erp_cust_az12 ec
on ci.cst_key = ec.cid
left join silver.erp_loc_a101 el
on ci.cst_key = el.cid)t

group by cst_id
having count(*) > 1

--intergrate gen and validate

select distinct new_gen
from (
select distinct
	ci.cst_gndr,
	ec.gen,
	case 
		when ci.cst_gndr = 'n/a' then coalesce(ec.gen,'n/a')
		else ci.cst_gndr
	end as new_gen


from silver.crm_cust_info ci
left join silver.erp_cust_az12 ec
on ci.cst_key = ec.cid
left join silver.erp_loc_a101 el
on ci.cst_key = el.cid
)t


--check customers view
select distinct gender from gold.dim_customers 
select * from gold.dim_customers
--cst_gndr is main branch

select prd_key,count(*) from(
select 
	cp.prd_id,
	cp.cat_id,
	cp.prd_key,
	cp.prd_nm,
	cp.prd_cost,
	cp.prd_line,
	cp.prd_start_dt,
	ep.cat,
	ep.subcat,
	ep.maintenance
from silver.crm_prd_info cp
left join silver.erp_px_cat_g1v2 ep
on cp.cat_id = ep.id
where cp.prd_end_dt is null
)t
group by prd_key
having count(*) > 1


SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL 
