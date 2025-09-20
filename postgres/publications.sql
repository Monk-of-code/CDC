-- Fixed publications script
\c transactions

-- Drop publications if they exist
DO $$
BEGIN
    DROP PUBLICATION hr_pub;
EXCEPTION WHEN undefined_object THEN
    RAISE NOTICE 'hr_pub publication did not exist';
END
$$;

DO $$
BEGIN
    DROP PUBLICATION ecommerce_pub;
EXCEPTION WHEN undefined_object THEN
    RAISE NOTICE 'ecommerce_pub publication did not exist';
END
$$;

-- Create publications for each table individually
CREATE PUBLICATION hr_pub FOR TABLE 
    hr.departments,
    hr.employees,
    hr.attendance,
    hr.salaries,
    hr.projects,
    hr.employee_projects;

CREATE PUBLICATION ecommerce_pub FOR TABLE 
    ecommerce.customers,
    ecommerce.products,
    ecommerce.orders,
    ecommerce.order_items,
    ecommerce.payments,
    ecommerce.shipments;

-- Verify publications
SELECT 
    p.pubname as publication_name,
    (SELECT COUNT(*) FROM pg_publication_tables WHERE pubname = p.pubname) as table_count
FROM pg_publication p;

-- Show detailed publication info
SELECT 
    pubname as publication,
    schemaname as schema,
    tablename as table
FROM pg_publication_tables 
ORDER BY pubname, schemaname, tablename;