CREATE DATABASE IF NOT EXISTS demo123;
USE demo123;

-- Drop tables only inside THIS database (safe)
DROP TABLE IF EXISTS sales_fact_v2;
DROP TABLE IF EXISTS supplier_dim_v2;
DROP TABLE IF EXISTS store_dim_v2;
DROP TABLE IF EXISTS date_dim_v2;
DROP TABLE IF EXISTS product_dim_v2;
DROP TABLE IF EXISTS customer_dim_v2;

----------------------------------------------------------
-- DIMENSION: CUSTOMER
----------------------------------------------------------
CREATE TABLE customer_dim_v2 (
  customer_sk INT AUTO_INCREMENT PRIMARY KEY,
  customer_id VARCHAR(50) UNIQUE,
  gender VARCHAR(10),
  age_group VARCHAR(20),
  occupation VARCHAR(50),
  city_category VARCHAR(10),
  stay_in_current_city_years VARCHAR(5),
  marital_status INT,
  INDEX idx_cust_id_v2 (customer_id),
  INDEX idx_cust_gender_v2 (gender)
);

----------------------------------------------------------
-- DIMENSION: PRODUCT
----------------------------------------------------------
CREATE TABLE product_dim_v2 (
  product_sk INT AUTO_INCREMENT PRIMARY KEY,
  product_id VARCHAR(50) UNIQUE,
  product_category VARCHAR(100),
  price DECIMAL(10,2),
  store_id VARCHAR(20),
  supplier_id VARCHAR(20),
  store_name VARCHAR(100),
  supplier_name VARCHAR(100),
  INDEX idx_prod_id_v2 (product_id),
  INDEX idx_prod_cat_v2 (product_category)
);

----------------------------------------------------------
-- DIMENSION: STORE
----------------------------------------------------------
CREATE TABLE store_dim_v2 (
  store_sk INT AUTO_INCREMENT PRIMARY KEY,
  store_id VARCHAR(20) UNIQUE,
  store_name VARCHAR(100),
  INDEX idx_store_id_v2 (store_id)
);

----------------------------------------------------------
-- DIMENSION: SUPPLIER
----------------------------------------------------------
CREATE TABLE supplier_dim_v2 (
    supplier_sk INT AUTO_INCREMENT PRIMARY KEY,
    supplier_id VARCHAR(20) UNIQUE,
    supplier_name VARCHAR(128),
    INDEX idx_supplier_id_v2 (supplier_id)
);

----------------------------------------------------------
-- DIMENSION: DATE
----------------------------------------------------------
CREATE TABLE date_dim_v2 (
  date_sk INT AUTO_INCREMENT PRIMARY KEY,
  date DATE UNIQUE,
  year INT,
  month INT,
  day INT,
  INDEX idx_date_v2 (date)
);

----------------------------------------------------------
-- FACT TABLE: SALES
----------------------------------------------------------
CREATE TABLE sales_fact_v2 (
  sale_id INT AUTO_INCREMENT PRIMARY KEY,
  order_id VARCHAR(50),
  customer_sk INT,
  product_sk INT,
  store_sk INT,
  supplier_sk INT,
  date_sk INT,
  quantity INT,
  total_price DECIMAL(10,2),

  FOREIGN KEY(customer_sk) REFERENCES customer_dim_v2(customer_sk),
  FOREIGN KEY(product_sk) REFERENCES product_dim_v2(product_sk),
  FOREIGN KEY(store_sk) REFERENCES store_dim_v2(store_sk),
  FOREIGN KEY(supplier_sk) REFERENCES supplier_dim_v2(supplier_sk),
  FOREIGN KEY(date_sk) REFERENCES date_dim_v2(date_sk),
  
  INDEX idx_order_v2 (order_id),
  INDEX ix_fact_cust_v2 (customer_sk),
  INDEX ix_fact_prod_v2 (product_sk),
  INDEX ix_fact_store_v2 (store_sk),
  INDEX ix_fact_supplier_v2 (supplier_sk)
);

SELECT COUNT(*) AS total_records FROM sales_fact_v2;


SELECT DISTINCT YEAR(date) FROM date_dim_v2 ORDER BY 1; #to check the avaialble years 
# 1
WITH sales_enriched AS (
    SELECT
        p.product_id,
        p.product_category,
        p.product_id AS product_name, -- no separate name in your dataset
        sf.total_price,
        dd.date,
        dd.year AS yr,
        dd.month AS mon,
        CASE 
            WHEN DAYOFWEEK(dd.date) IN (1,7) THEN 'Weekend'
            ELSE 'Weekday'
        END AS day_type
    FROM sales_fact_v2 sf
    JOIN date_dim_v2 dd      ON sf.date_sk = dd.date_sk
    JOIN product_dim_v2 p    ON sf.product_sk = p.product_sk
    WHERE dd.year = 2015
),

monthly_product_rev AS (
    SELECT
        yr, mon, day_type,
        product_id,
        SUM(total_price) AS revenue
    FROM sales_enriched
    GROUP BY yr, mon, day_type, product_id
),

ranked AS (
    SELECT
        yr, mon, day_type, product_id, revenue,
        ROW_NUMBER() OVER (
            PARTITION BY yr, mon, day_type
            ORDER BY revenue DESC
        ) AS rn
    FROM monthly_product_rev
)

SELECT
    yr AS year,
    mon AS month,
    day_type,
    product_id,
    revenue
FROM ranked
WHERE rn <= 5
ORDER BY yr, mon, day_type, revenue DESC;

#2
SELECT
    COALESCE(dc.gender, 'Unknown') AS gender,
    COALESCE(dc.age_group, 'Unknown') AS age_group,
    COALESCE(dc.city_category, 'Unknown') AS city_category,
    SUM(sf.total_price) AS total_revenue,
    SUM(sf.quantity)    AS total_quantity,
    COUNT(DISTINCT sf.order_id) AS orders_count
FROM sales_fact_v2 sf
JOIN customer_dim_v2 dc ON sf.customer_sk = dc.customer_sk
GROUP BY gender, age_group, city_category
ORDER BY total_revenue DESC;


#3
SELECT
    COALESCE(p.product_category, 'Unknown') AS product_category,
    COALESCE(dc.occupation, 'Unknown') AS occupation,
    SUM(sf.total_price) AS revenue,
    SUM(sf.quantity) AS qty
FROM sales_fact_v2 sf
JOIN product_dim_v2 p ON sf.product_sk = p.product_sk
JOIN customer_dim_v2 dc ON sf.customer_sk = dc.customer_sk
GROUP BY p.product_category, dc.occupation
ORDER BY p.product_category, revenue DESC;

# 4
SELECT
    COALESCE(dc.gender, 'Unknown') AS gender,
    COALESCE(dc.age_group, 'Unknown') AS age_group,
    QUARTER(dd.date) AS quarter,
    SUM(sf.total_price) AS revenue,
    SUM(sf.quantity) AS quantity
FROM sales_fact_v2 sf
JOIN customer_dim_v2 dc ON sf.customer_sk = dc.customer_sk
JOIN date_dim_v2 dd ON sf.date_sk = dd.date_sk
WHERE dd.year = 2019  -- SELECT THE YEAR 2015-2020
GROUP BY gender, age_group, quarter
ORDER BY quarter, gender, age_group;

# 5
WITH occ_rev AS (
    SELECT
        p.product_category,
        dc.occupation,
        SUM(sf.total_price) AS revenue
    FROM sales_fact_v2 sf
    JOIN product_dim_v2 p ON sf.product_sk = p.product_sk
    JOIN customer_dim_v2 dc ON sf.customer_sk = dc.customer_sk
    GROUP BY p.product_category, dc.occupation
)
SELECT
    product_category,
    occupation,
    revenue
FROM (
    SELECT
        product_category,
        occupation,
        revenue,
        ROW_NUMBER() OVER (PARTITION BY product_category ORDER BY revenue DESC) AS rn
    FROM occ_rev
) t
WHERE rn <= 5  -- Top 5 occupations per product category
ORDER BY product_category, revenue DESC;

# 6

WITH ranked_months AS (
    SELECT 
        year,
        month,
        ROW_NUMBER() OVER (ORDER BY year DESC, month DESC) AS rn
    FROM (
        SELECT DISTINCT year, month
        FROM date_dim_v2
    ) t
)
SELECT
    dc.city_category,
    dc.marital_status,
    dd.year,
    dd.month,
    SUM(sf.total_price) AS total_revenue,
    SUM(sf.quantity) AS total_quantity,
    COUNT(DISTINCT sf.order_id) AS orders_count
FROM sales_fact_v2 sf
JOIN customer_dim_v2 dc ON sf.customer_sk = dc.customer_sk
JOIN date_dim_v2 dd ON sf.date_sk = dd.date_sk
JOIN ranked_months rm ON dd.year = rm.year AND dd.month = rm.month
WHERE rm.rn <= 6  -- dynamically pick last 6 months in data
GROUP BY dc.city_category, dc.marital_status, dd.year, dd.month
ORDER BY dd.year, dd.month, dc.city_category, dc.marital_status;

# 7 
SELECT
    COALESCE(dc.stay_in_current_city_years, 'Unknown') AS stay_years,
    COALESCE(dc.gender, 'Unknown') AS gender,
    AVG(fs.total_price) AS avg_purchase_amount
FROM sales_fact_v2 fs
JOIN customer_dim_v2 dc ON fs.customer_sk = dc.customer_sk
GROUP BY stay_years, gender
ORDER BY stay_years, gender;


# 8 
WITH cat_city_rev AS (
    SELECT
        p.product_category,
        dc.city_category,
        SUM(fs.total_price) AS revenue
    FROM sales_fact_v2 fs
    JOIN customer_dim_v2 dc ON fs.customer_sk = dc.customer_sk
    JOIN product_dim_v2 p ON fs.product_sk = p.product_sk
    GROUP BY p.product_category, dc.city_category
)
SELECT *
FROM (
    SELECT
        product_category,
        city_category,
        revenue,
        ROW_NUMBER() OVER (PARTITION BY product_category ORDER BY revenue DESC) AS rn
    FROM cat_city_rev
) t
WHERE rn <= 5
ORDER BY product_category, revenue DESC;

#9
WITH monthly_rev AS (
    SELECT
        p.product_category,
        dd.year AS yr,
        dd.month AS mon,
        SUM(sf.total_price) AS revenue
    FROM sales_fact_v2 sf
    JOIN date_dim_v2 dd ON sf.date_sk = dd.date_sk
    JOIN product_dim_v2 p ON sf.product_sk = p.product_sk
    WHERE dd.year = 2020
    GROUP BY p.product_category, yr, mon
),
growth AS (
    SELECT
        product_category,
        yr,
        mon,
        revenue,
        LAG(revenue) OVER (
            PARTITION BY product_category ORDER BY mon
        ) AS prev_revenue
    FROM monthly_rev
)
SELECT
    product_category,
    yr,
    mon,
    revenue,
    ROUND(((revenue - prev_revenue) / prev_revenue) * 100, 2) AS mom_growth_percent
FROM growth
ORDER BY product_category, mon;

#10
SELECT
    dc.age_group,
    CASE 
        WHEN DAYOFWEEK(dd.date) IN (1,7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    SUM(sf.total_price) AS total_revenue
FROM sales_fact_v2 sf
JOIN customer_dim_v2 dc ON sf.customer_sk = dc.customer_sk
JOIN date_dim_v2 dd ON sf.date_sk = dd.date_sk
WHERE dd.year = 2020
GROUP BY dc.age_group, day_type
ORDER BY dc.age_group, day_type;

#11
WITH enriched AS (
    SELECT
        p.product_id,
        p.product_category,
        dd.month AS mon,
        CASE 
            WHEN DAYOFWEEK(dd.date) IN (1,7) THEN 'Weekend'
            ELSE 'Weekday'
        END AS day_type,
        sf.total_price
    FROM sales_fact_v2 sf
    JOIN date_dim_v2 dd ON sf.date_sk = dd.date_sk
    JOIN product_dim_v2 p ON sf.product_sk = p.product_sk
    WHERE dd.year = 2020
),
monthly_rank AS (
    SELECT
        product_id,
        product_category,
        mon,
        day_type,
        SUM(total_price) AS revenue,
        ROW_NUMBER() OVER (
            PARTITION BY mon, day_type
            ORDER BY SUM(total_price) DESC
        ) AS rn
    FROM enriched
    GROUP BY product_id, product_category, mon, day_type
)
SELECT
    mon,
    day_type,
    product_id,
    product_category,
    revenue
FROM monthly_rank
WHERE rn <= 5
ORDER BY mon, day_type, revenue DESC;

#12
WITH store_qtr AS (
    SELECT
        p.store_name,
        dd.year AS yr,
        QUARTER(dd.date) AS qtr,
        SUM(sf.total_price) AS revenue
    FROM sales_fact_v2 sf
    JOIN product_dim_v2 p ON sf.product_sk = p.product_sk
    JOIN date_dim_v2 dd ON sf.date_sk = dd.date_sk
    WHERE dd.year = 2017
    GROUP BY p.store_name, yr, qtr
),
growth AS (
    SELECT
        store_name,
        yr,
        qtr,
        revenue,
        LAG(revenue) OVER (
            PARTITION BY store_name ORDER BY qtr
        ) AS prev_qtr_revenue
    FROM store_qtr
)
SELECT
    store_name,
    yr,
    qtr,
    revenue,
    ROUND(((revenue - prev_qtr_revenue) / prev_qtr_revenue) * 100, 2) AS growth_rate_percent
FROM growth
ORDER BY store_name, qtr; #null in growth means 0.000 growth

#13
SELECT 
    st.store_id,
    st.store_name,
    sup.supplier_id,
    sup.supplier_name,
    p.product_id,
    p.product_category,
    
    SUM(sf.total_price) AS total_sales,
    SUM(sf.quantity) AS total_quantity,
    COUNT(*) AS transaction_count,
    AVG(sf.total_price) AS avg_transaction_value

FROM sales_fact_v2 sf
JOIN store_dim_v2 st      ON sf.store_sk = st.store_sk
JOIN supplier_dim_v2 sup  ON sf.supplier_sk = sup.supplier_sk
JOIN product_dim_v2 p     ON sf.product_sk = p.product_sk

WHERE st.store_id IS NOT NULL
  AND sup.supplier_id IS NOT NULL

GROUP BY 
    st.store_id, 
    st.store_name,
    sup.supplier_id, 
    sup.supplier_name,
    p.product_id,
    p.product_category

ORDER BY 
    st.store_name,
    sup.supplier_name,
    p.product_category,
    total_sales DESC;
    
#14
SELECT 
    p.product_id,
    p.product_category,

    CASE 
        WHEN dd.month IN (3, 4, 5) THEN 'Spring'
        WHEN dd.month IN (6, 7, 8) THEN 'Summer'
        WHEN dd.month IN (9, 10, 11) THEN 'Fall'
        WHEN dd.month IN (12, 1, 2) THEN 'Winter'
    END AS season,

    SUM(sf.total_price) AS total_sales,
    SUM(sf.quantity) AS total_quantity,
    COUNT(*) AS transaction_count,
    AVG(sf.total_price) AS avg_transaction_value

FROM sales_fact_v2 sf
JOIN product_dim_v2 p ON sf.product_sk = p.product_sk
JOIN date_dim_v2 dd   ON sf.date_sk = dd.date_sk

GROUP BY 
    p.product_id, 
    p.product_category, 
    season

HAVING season IS NOT NULL   -- exclude null values safely

ORDER BY 
    p.product_category,
    p.product_id,
    CASE season
        WHEN 'Spring' THEN 1
        WHEN 'Summer' THEN 2
        WHEN 'Fall' THEN 3
        WHEN 'Winter' THEN 4
    END;
    
    #15
WITH monthly_revenue AS (
    SELECT 
        ds.store_id,
        ds.store_name,
        sup.supplier_id,
        sup.supplier_name,
        dd.year,
        dd.month,
        SUM(sf.total_price) AS monthly_revenue
    FROM sales_fact_v2 sf
    JOIN store_dim_v2 ds 
        ON sf.store_sk = ds.store_sk
    JOIN supplier_dim_v2 sup 
        ON sf.supplier_sk = sup.supplier_sk
    JOIN date_dim_v2 dd 
        ON sf.date_sk = dd.date_sk
    WHERE ds.store_id IS NOT NULL
      AND sup.supplier_id IS NOT NULL
    GROUP BY 
        ds.store_id, ds.store_name,
        sup.supplier_id, sup.supplier_name,
        dd.year, dd.month
),
volatility_calc AS (
    SELECT 
        mr1.store_id,
        mr1.store_name,
        mr1.supplier_id,
        mr1.supplier_name,
        mr1.year,
        mr1.month AS current_month,
        mr1.monthly_revenue AS current_revenue,
        mr2.month AS previous_month,
        mr2.monthly_revenue AS previous_revenue,
        CASE 
            WHEN mr2.monthly_revenue > 0 THEN 
                ABS(((mr1.monthly_revenue - mr2.monthly_revenue) 
                     / mr2.monthly_revenue) * 100)
            ELSE NULL
        END AS volatility_percentage
    FROM monthly_revenue mr1
    LEFT JOIN monthly_revenue mr2 
        ON mr1.store_id = mr2.store_id
        AND mr1.supplier_id = mr2.supplier_id
        AND mr1.year = mr2.year
        AND mr1.month = mr2.month + 1
)
SELECT 
    store_id,
    store_name,
    supplier_id,
    supplier_name,
    year,
    current_month,
    current_revenue,
    previous_month,
    previous_revenue,
    COALESCE(volatility_percentage, 0) AS volatility_percentage
FROM volatility_calc
ORDER BY store_name, supplier_name, year DESC, current_month DESC;

# 16
WITH product_pairs AS (
    SELECT
        LEAST(p1.product_id, p2.product_id) AS product_a,
        GREATEST(p1.product_id, p2.product_id) AS product_b,
        fs1.order_id
    FROM sales_fact_v2 fs1
    JOIN sales_fact_v2 fs2 
        ON fs1.order_id = fs2.order_id
       AND fs1.product_sk < fs2.product_sk  -- ensures unique pairs
    JOIN product_dim_v2 p1 ON fs1.product_sk = p1.product_sk
    JOIN product_dim_v2 p2 ON fs2.product_sk = p2.product_sk
)
SELECT
    product_a,
    product_b,
    COUNT(*) AS times_bought_together
FROM product_pairs
GROUP BY product_a, product_b
ORDER BY times_bought_together DESC
LIMIT 5;  ## no output in this so it means there is no transaction in which there is more than 1 product purchased and so pairs will not be forming in this case hence empty output

#17
SELECT 
    dd.year,
    sd.store_name,
    sup.supplier_name,
    pd.product_category,
    SUM(sf.total_price) AS total_revenue,
    SUM(sf.quantity) AS total_quantity,
    COUNT(*) AS transaction_count
FROM sales_fact_v2 sf
JOIN date_dim_v2 dd ON sf.date_sk = dd.date_sk
JOIN store_dim_v2 sd ON sf.store_sk = sd.store_sk
JOIN supplier_dim_v2 sup ON sf.supplier_sk = sup.supplier_sk
JOIN product_dim_v2 pd ON sf.product_sk = pd.product_sk
GROUP BY 
    dd.year,
    sd.store_name,
    sup.supplier_name,
    pd.product_category
WITH ROLLUP
ORDER BY 
    dd.year,
    sd.store_name,
    sup.supplier_name,
    pd.product_category;

#18
SELECT
    p.product_id,
    p.product_category,
    
    -- H1 totals
    SUM(CASE WHEN MONTH(dd.date) BETWEEN 1 AND 6 THEN sf.total_price END) AS h1_revenue,
    SUM(CASE WHEN MONTH(dd.date) BETWEEN 1 AND 6 THEN sf.quantity END) AS h1_quantity,
    
    -- H2 totals
    SUM(CASE WHEN MONTH(dd.date) BETWEEN 7 AND 12 THEN sf.total_price END) AS h2_revenue,
    SUM(CASE WHEN MONTH(dd.date) BETWEEN 7 AND 12 THEN sf.quantity END) AS h2_quantity,

    -- Full year totals
    SUM(sf.total_price) AS yearly_revenue,
    SUM(sf.quantity) AS yearly_quantity

FROM sales_fact_v2 sf
JOIN product_dim_v2 p ON sf.product_sk = p.product_sk
JOIN date_dim_v2 dd ON sf.date_sk = dd.date_sk
GROUP BY p.product_id, p.product_category
ORDER BY yearly_revenue DESC;

#19
#identifying High Revenue Spikes in Product Sales and Highlight Outliers
WITH daily_product_sales AS (
    SELECT 
        p.product_id,
        p.product_category,
        dd.date AS date_value,
        SUM(sf.total_price) AS daily_revenue,
        SUM(sf.quantity) AS daily_quantity
    FROM sales_fact_v2 sf
    JOIN product_dim_v2 p ON sf.product_sk = p.product_sk
    JOIN date_dim_v2 dd ON sf.date_sk = dd.date_sk
    GROUP BY p.product_id, p.product_category, dd.date
),
product_avg_sales AS (
    SELECT 
        product_id,
        product_category,
        AVG(daily_revenue) AS avg_daily_revenue
    FROM daily_product_sales
    GROUP BY product_id, product_category
)
SELECT 
    dps.product_id,
    dps.product_category,
    dps.date_value,
    dps.daily_revenue,
    pas.avg_daily_revenue,
    CASE 
        WHEN dps.daily_revenue > (pas.avg_daily_revenue * 2) THEN 'OUTLIER - Spike Detected'
        ELSE 'Normal'
    END AS anomaly_flag,
    CASE 
        WHEN dps.daily_revenue > (pas.avg_daily_revenue * 2) THEN 
            CONCAT('Sales spike: ', ROUND(dps.daily_revenue, 2), ' exceeds 2x average of ', ROUND(pas.avg_daily_revenue, 2))
        ELSE NULL
    END AS explanation
FROM daily_product_sales dps
JOIN product_avg_sales pas 
    ON dps.product_id = pas.product_id
WHERE dps.daily_revenue > (pas.avg_daily_revenue * 2)
ORDER BY dps.product_category, dps.product_id, dps.date_value DESC;

#19
# Identify daily sales spikes (2x above average) for each product

WITH daily_sales AS (
    SELECT
        p.product_id,
        dd.date AS date_value,
        SUM(sf.total_price) AS daily_revenue
    FROM sales_fact_v2 sf
    JOIN date_dim_v2 dd ON sf.date_sk = dd.date_sk
    JOIN product_dim_v2 p ON sf.product_sk = p.product_sk
    GROUP BY p.product_id, dd.date
),
avg_sales AS (
    SELECT
        product_id,
        AVG(daily_revenue) AS avg_daily_revenue
    FROM daily_sales
    GROUP BY product_id
),
spikes AS (
    SELECT
        d.product_id,
        d.date_value,
        d.daily_revenue,
        a.avg_daily_revenue,
        CASE
            WHEN d.daily_revenue > 2 * a.avg_daily_revenue THEN 'Spike / Outlier'
            ELSE 'Normal'
        END AS status,
        ROUND((d.daily_revenue / a.avg_daily_revenue), 2) AS multiple_of_avg
    FROM daily_sales d
    JOIN avg_sales a ON d.product_id = a.product_id
)
SELECT *
FROM spikes
ORDER BY product_id, date_value; # if daily revenue >2*avg_daily_revenue then its outlier in this logical query else normal

#20
#create a View STORE_QUARTERLY_SALES for Optimized Sales Analysis

DROP VIEW IF EXISTS STORE_QUARTERLY_SALES;

CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT 
    s.store_sk,
    s.store_id,
    s.store_name,
    d.year,
    QUARTER(d.date) AS quarter,
    SUM(sf.total_price) AS quarterly_revenue,
    SUM(sf.quantity) AS quarterly_quantity,
    COUNT(*) AS transaction_count,
    AVG(sf.total_price) AS avg_transaction_value,
    COUNT(DISTINCT sf.customer_sk) AS unique_customers
FROM sales_fact_v2 sf
JOIN store_dim_v2 s 
    ON sf.store_sk = s.store_sk
JOIN date_dim_v2 d 
    ON sf.date_sk = d.date_sk
WHERE s.store_id IS NOT NULL
  AND d.date IS NOT NULL
GROUP BY 
    s.store_sk,
    s.store_id,
    s.store_name, 
    d.year, 
    quarter;

-- Query the view
SELECT * 
FROM STORE_QUARTERLY_SALES
ORDER BY store_name, year DESC, quarter DESC;







