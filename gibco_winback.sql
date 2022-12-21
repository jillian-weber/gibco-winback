DROP TABLE IF EXISTS workflow;
CREATE TEMPORARY TABLE workflow AS
SELECT sku_number,
       listagg(workflow_master.keycode, '|') WITHIN GROUP ( ORDER BY workflow_master.keycode )     AS keycodes,
       listagg(workflow_family, '|') WITHIN GROUP ( ORDER BY workflow_master.keycode )             AS workflow_family,
       listagg((CASE
                    WHEN level06_name IS NOT NULL THEN level06_name
                    WHEN level05_name IS NOT NULL THEN level06_name
                    WHEN level04_name IS NOT NULL THEN level06_name
                    WHEN level03_name IS NOT NULL THEN level06_name
                    ELSE level02_name END), '|') WITHIN GROUP ( ORDER BY workflow_master.keycode ) AS keycode_names
FROM lsg_product.workflow_master
         INNER JOIN lsg_product.workflow_to_sku ON workflow_master.keycode = workflow_to_sku.keycode
GROUP BY sku_number;

DROP TABLE IF EXISTS stage_losses;
create temp table stage_losses
(
    location_uid      varchar(50),
    packsize_uid      integer,
    sku_number        varchar(50),
    sku_name          varchar(255),
    sku_size          varchar(200),
    product_line_code varchar(10),
    product_line_name varchar(255),
    product_line_group_code varchar(255),
    product_line_group_name varchar(255),
    global_region_code       varchar(255),
    global_region_name       varchar(255),
    portfolio         varchar(200),
    keycodes          varchar(200),
    workflow_family   varchar(200),
    keycode_names     varchar(255),
    last_revenue      numeric(23, 2),
    last_quantity     integer,
    last_order        date,
    recurrence        numeric(23, 2),
    -- rev_15m, _3m, etc are all revenue's from the output date with a lookback period of the amount of months specified.
    rev_15m           numeric(23, 2),
    rev_3m            numeric(23, 2),
    rev_1m            numeric(23, 2),
    -- revenue of the year of 2019 (precovid in this analysis will always be the year 2019)
    rev_precovid      numeric(23, 2),
    rev_12m           numeric(23, 2),
    -- average quarterly revenue for 2019, the past 15m from output date, and past 12m from output date
    avg_quart_precovid numeric(23, 2),
    avg_quart_15m     numeric(23,2),
    avg_quart_12m     numeric(23,2),
    -- both opportunity calculations use the same equation but with different baselines (15m or precovid)
    opportunity       numeric(23, 2),
    opp_rev_precovid  numeric(23, 2),
    revenue           integer,
    inertia           integer,
    frequency         integer,
    recency           integer,
    repeat            integer,
    score             numeric(23, 2),
    price_realization numeric(23, 2),
    -- customer_classification is for "lost", "declining", "increasing", "other"
    customer_classification varchar(50),
    -- is_new is t/f value for if a customers first purchase was after 2019 (01-01-2020 or later)
    is_new  boolean,
    -- classification 12m has the same classifications as customer_classification column,
    -- BUT this column is based on just a 12m lookback baseline instead of precovid,
    -- this column compared against customer_classification are intended for and can be used for a confusion matrix to understand the output of data
    classification_12m  varchar(50),
    created_at        timestamptz default (current_timestamp)
);



INSERT INTO stage_losses (location_uid, packsize_uid, sku_number, sku_name, sku_size, product_line_code,
                          product_line_name, product_line_group_code, product_line_group_name, global_region_code, global_region_name, portfolio, keycodes, workflow_family,
                          keycode_names,
                          last_revenue, last_quantity, last_order, recurrence, rev_15m, rev_3m, rev_1m,
                          rev_precovid, rev_12m, avg_quart_precovid, avg_quart_15m, avg_quart_12m,
                          opportunity, opp_rev_precovid, revenue, inertia, frequency, recency, repeat,
                          score, customer_classification, is_new, classification_12m, price_realization)

WITH labs AS (WITH lab_sales AS (SELECT location_uid,
                                        global_region_code,
                                        global_region_name,
                                        sum(lsg_net_sales)  AS rev,
                                        sum(CASE
                                                WHEN ship_date BETWEEN (current_date - INTERVAL '120 days') AND (current_date - INTERVAL '60 days')
                                                    THEN lsg_net_sales
                                                ELSE 0 END) AS p6m_rev,
                                        sum(CASE
                                                WHEN ship_date BETWEEN (current_date - INTERVAL '60 days') AND current_date
                                                    THEN lsg_net_sales
                                                ELSE 0 END) AS c6m_rev
                                 FROM lsg_sales.tf_transactions_mapped
                                          INNER JOIN lsg_laboratory.combined_delivery
                                                     ON combined_delivery.del_uid = tf_transactions_mapped.del_uid
                               --  WHERE ship_date > (current_date - INTERVAL '15 months')
                                 GROUP BY location_uid, global_region_code, global_region_name)
              SELECT location_uid,
                     global_region_code,
                     global_region_name,
                     ntile(5) OVER (ORDER BY rev)                 AS rev_rank,
                     ntile(5) OVER (ORDER BY (c6m_rev - p6m_rev)) AS growth_rank
              FROM lab_sales),

     skus AS (WITH sku_sales AS (SELECT location_uid,
                                        packsize_uid,
                                        count(DISTINCT order_number)    AS orders,
                                        (current_date - max(ship_date)) AS last_order
                                 FROM lsg_sales.tf_transactions_mapped
                                          INNER JOIN lsg_laboratory.combined_delivery
                                                     ON combined_delivery.del_uid = tf_transactions_mapped.del_uid
                                          INNER JOIN lsg_product.master_sku
                                                     ON tf_transactions_mapped.sku_number = master_sku.sku_number
                               --  WHERE ship_date > (current_date - INTERVAL '15 months')
                                 GROUP BY location_uid,
                                          packsize_uid)
              SELECT location_uid,
                     packsize_uid,
                     ntile(5) OVER (PARTITION BY packsize_uid ORDER BY orders)          AS orders_rank,
                     ntile(5) OVER (PARTITION BY packsize_uid ORDER BY last_order DESC) AS recency_rank
              FROM sku_sales),

     dates AS (WITH datediff AS (WITH date_sales AS (SELECT location_uid,
                                                            packsize_uid,
                                                            ship_date,
                                                            row_number()
                                                            OVER (PARTITION BY location_uid, packsize_uid ORDER BY ship_date) AS row_num
                                                     FROM lsg_sales.tf_transactions_mapped
                                                              INNER JOIN lsg_laboratory.combined_delivery
                                                                         ON combined_delivery.del_uid = tf_transactions_mapped.del_uid
                                                              INNER JOIN lsg_product.master_sku
                                                                         ON tf_transactions_mapped.sku_number = master_sku.sku_number
                                                    -- WHERE ship_date > (current_date - INTERVAL '15 months')
                                                     GROUP BY location_uid,
                                                              packsize_uid,
                                                              ship_date)
                                 SELECT trigger.location_uid,
                                        trigger.packsize_uid,
                                        avg(repeat.ship_date - trigger.ship_date) AS avg_datediff
                                 FROM date_sales AS trigger
                                          INNER JOIN date_sales AS repeat
                                                     ON repeat.location_uid = trigger.location_uid AND
                                                        repeat.packsize_uid = trigger.packsize_uid AND
                                                        trigger.row_num = (repeat.row_num - 1)
                                 GROUP BY trigger.location_uid,
                                          trigger.packsize_uid
                                 ORDER BY trigger.location_uid,
                                          trigger.packsize_uid)
               SELECT location_uid,
                      packsize_uid,
                      ntile(5) OVER (PARTITION BY packsize_uid ORDER BY avg_datediff DESC) AS datediff_rank
               FROM datediff),
     lasts AS (SELECT DISTINCT location_uid,
                               packsize_uid,
                               last_value(lsg_net_sales)
                               OVER (PARTITION BY location_uid, packsize_uid ORDER BY ship_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_rev,
                               last_value(lsg_ship_quantity)
                               OVER (PARTITION BY location_uid, packsize_uid ORDER BY ship_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_qty,
                               last_value(tx.sku_number)
                               OVER (PARTITION BY location_uid, packsize_uid ORDER BY ship_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_sku,
                               last_value(ship_date)
                               OVER (PARTITION BY location_uid, packsize_uid ORDER BY ship_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_order
               FROM lsg_sales.tf_transactions_mapped tx
                        INNER JOIN lsg_laboratory.combined_delivery ON combined_delivery.del_uid = tx.del_uid
                        INNER JOIN lsg_product.master_sku ON tx.sku_number = master_sku.sku_number
              -- WHERE ship_date > (current_date - INTERVAL '15 months')
               ORDER BY location_uid,
                        packsize_uid DESC),
     losses AS (SELECT location_uid,
                       packsize_uid,
                       min(ship_date)                                AS first_sales,
                       max(ship_date)                                AS last_sales,
                       (max(ship_date) - min(ship_date))::FLOAT      AS days_between,
                       sum(CASE
                               WHEN ship_date BETWEEN (current_date - INTERVAL '15 months') AND current_date
                                   THEN lsg_net_sales
                               ELSE 0 END)                           AS rev_15m,
                       sum(CASE
                               WHEN ship_date BETWEEN (current_date - INTERVAL '3 months') AND current_date
                                   THEN lsg_net_sales
                               ELSE 0 END)                           AS rev_3m,
                       sum(CASE
                               WHEN ship_date BETWEEN (current_date - INTERVAL '30 days') AND current_date
                                   THEN lsg_net_sales
                               ELSE 0 END)                           AS rev_1m,
                    --- adding pre covid revenue
                       sum(CASE
                               WHEN ship_date BETWEEN '2019-01-01'::DATE AND '2019-12-31'::DATE
                                    THEN lsg_net_sales
                               ELSE 0 END)                          AS rev_precovid,
                       sum(CASE
                               WHEN ship_date BETWEEN (current_date - INTERVAL '12 months') AND current_date
                                    THEN lsg_net_sales
                               ELSE 0 END)                          AS rev_12m,

                       sum(CASE
                               WHEN ship_date BETWEEN (current_date - INTERVAL '15 months') AND current_date
                                   THEN lsg_ship_quantity
                               ELSE 0 END)                           AS qty_15m,
                       sum(CASE
                               WHEN ship_date BETWEEN (current_date - INTERVAL '3 months') AND current_date
                                   THEN lsg_ship_quantity
                               ELSE 0 END)                           AS qty_3m,
                       sum(CASE
                               WHEN ship_date BETWEEN (current_date - INTERVAL '30 days') AND current_date
                                   THEN lsg_ship_quantity
                               ELSE 0 END)                           AS qty_1m,
                       count(DISTINCT CASE
                                          WHEN ship_date BETWEEN (current_date - INTERVAL '15 months') AND current_date
                                              THEN order_number END) AS order_15m,
                       count(DISTINCT CASE
                                          WHEN ship_date BETWEEN (current_date - INTERVAL '3 months') AND current_date
                                              THEN order_number END) AS order_3m,
                       count(DISTINCT CASE
                                          WHEN ship_date BETWEEN (current_date - INTERVAL '30 days') AND current_date
                                              THEN order_number END) AS order_1m,

                      (rev_precovid / 4.0)                           AS avg_quart_precovid,
                      (rev_15m / 5.0)                                AS avg_quart_15m,
                      (rev_12m / 4.0)                                AS avg_quart_12m,

                      -- if an account hasn't had any revenue in the past 12 months, then they are 'LOST', we don't care about them in this analysis.
                       CASE
                           WHEN rev_12m = 0
                                THEN 'LOST'
                       -- comparing against average quarterly so we are comparing against the same amounts of time.
                       -- if the avg_quart_precovid is greater than rev_3m, then they are 'DECLINING'
                       -- if the avg_quart_precovid is less than rev_3m, then they are 'INCREASING'
                           WHEN avg_quart_precovid > rev_3m
                                THEN 'DECLINING'
                           WHEN avg_quart_precovid < rev_3m
                                THEN 'INCREASING'
                       -- rather than grouping everything else into a 'NULL' bucket, looks cleaner to just call other instances 'OTHER'
                           ELSE 'OTHER'
                       END   AS customer_classification,


                    -- if a customers first purchase was on or after 01-01-2020 then "NEW"
                    -- "new" classification was moved to its own column since it didn't make sense to have both revenue and order date classifications in the same column.
                       min(ship_date) > '2019-12-31'::DATE AS is_new,


                       CASE
                            WHEN rev_12m = 0
                                 THEN 'LOST'
                            WHEN avg_quart_12m> rev_3m
                                 THEN 'DECLINING'
                            WHEN avg_quart_12m < rev_3m
                                 THEN 'INCREASING'
                            ELSE 'OTHER'
                       END AS classificiation_12m


                FROM lsg_sales.tf_transactions_mapped
                         INNER JOIN lsg_laboratory.combined_delivery
                                    ON combined_delivery.del_uid = tf_transactions_mapped.del_uid
                         INNER JOIN lsg_product.master_sku ON tf_transactions_mapped.sku_number = master_sku.sku_number
               -- WHERE ship_date > (current_date - INTERVAL '15 months')
                GROUP BY location_uid,
                         packsize_uid,
                         master_sku.sku_number
                -- the having clause ensures we're only looking at the relevant data (15 month look back & precovid) for this analysis.
                -- we compare to rev_3m months so our conditions are the same length of time - ie avg quarterly (3m average) and rev_3m (3 month revenue from the run date)
                HAVING (((rev_precovid / 4.0) > rev_3m) OR  ((rev_15m / 5.0) > rev_3m))
                -- the AND statement limits our opportunity to be greater than 1000, because of the -1000, we need to use the abs() function when
                -- calculating revenue opportunity so we can get accurate numbers. If we didn't use the abs() function below then all revenue opp's on both account and sum level would be negative.
                -- As is, the revenue opp calculation SHOULD almost always produce negative numbers since we are subtracting an average quarterly (which we anticipate to be the larger number) from
                -- the past 3 months of revenue. To reiterate, that is why the abs() is necessary.
                   AND (((rev_3m - (rev_precovid / 4.0)) < -1000) OR ((rev_3m - (rev_15m / 5.0)) < -1000))
                -- if the above HAVING clause were to ever be removed, then it would not make sense to maintain the abs() function when calculating revenue opportunity.
                ORDER BY location_uid,
                         packsize_uid DESC),

     sku_asp AS (SELECT DISTINCT rev.sku_number,
                                 addr.alctr_country                                                AS country,
                                 sum(rev.lsg_net_sales::FLOAT) / sum(rev.lsg_ship_quantity::FLOAT) AS asp
                 FROM lsg_sales.tf_transactions_mapped rev
                          INNER JOIN lsg_e1.addresses_f0116 addr ON rev.sgn_id = addr.alan8_account_number
                 WHERE rev.ship_date BETWEEN (current_date - INTERVAL '15 months') AND current_date
                 GROUP BY rev.sku_number,
                          addr.alctr_country
                 HAVING sum(rev.lsg_ship_quantity::FLOAT) <> 0),

     loc_country AS (SELECT DISTINCT loc.location_uid, addr.alctr_country AS country
                     FROM lsg_laboratory.combined_location loc
                              INNER JOIN lsg_e1.addresses_f0116 addr ON loc.sgn_number = addr.alan8_account_number)

SELECT DISTINCT losses.location_uid,
                losses.packsize_uid,
                lasts.last_sku,
                productname,
                product_size,
                product_hierarchy.product_line_code,
                product_hierarchy.product_line_name,
                product_hierarchy.product_line_group_code,
                product_hierarchy.product_line_group_name,
                global_region_code,
                global_region_name,
                dsr_product_sub_grouping                                                                            AS portfolio,
                keycodes,
                workflow_family,
                keycode_names,
                lasts.last_rev,
                lasts.last_qty,
                lasts.last_order,
                round(days_between / order_15m, 2)::FLOAT                                                           AS recurrence,
                rev_15m,
                rev_3m,
                rev_1m,
                rev_precovid,
                rev_12m,
                avg_quart_precovid,
                avg_quart_15m,
                avg_quart_12m,
                -- abs() function is necessary here based on the HAVING clause that limits the scope of data we are looking at,
                -- more specifically where there is a limit for opportunity to be greater than -1000.
                -- As is, the revenue opp calculation SHOULD almost always produce negative numbers since we are subtracting an average quarterly (which we anticipate to be the larger number) from
                -- the past 3 months of revenue. To reiterate, that is why the abs() is necessary.
                abs(rev_3m - (rev_15m / 5.0))                                                                       AS opp_rev,
                abs(rev_3m - (rev_precovid / 4.0))                                                                  AS opp_rev_precovid,
                labs.rev_rank                                                                                       AS revenue,
                labs.growth_rank                                                                                    AS inertia,
                skus.orders_rank                                                                                    AS frequency,
                skus.recency_rank                                                                                   AS recency,
                (CASE WHEN datediff_rank IS NULL THEN 0 ELSE datediff_rank END)                                     AS repeat,
                sqrt((revenue * revenue + inertia * inertia + frequency * frequency + recency * recency +
                      repeat * repeat) /5)                                                                          AS score, --RMS, not super necessary here, but fun to play with;
                customer_classification,
                is_new,
                classificiation_12m,
                100 * (CASE WHEN sku_asp.asp = 0 THEN 0
                           ELSE ((rev_15m / qty_15m) - sku_asp.asp) / sku_asp.asp END)                              AS price_realization
FROM losses
         INNER JOIN labs ON labs.location_uid = losses.location_uid
         INNER JOIN lasts ON losses.location_uid = lasts.location_uid AND lasts.packsize_uid = losses.packsize_uid
         LEFT JOIN skus ON labs.location_uid = skus.location_uid AND skus.packsize_uid = losses.packsize_uid
         LEFT JOIN dates ON labs.location_uid = dates.location_uid AND dates.packsize_uid = losses.packsize_uid
         LEFT JOIN lsg_product.product_master_active_only ON product_master_active_only.productid = lasts.last_sku
         LEFT JOIN lsg_product.product_hierarchy
                   ON product_hierarchy.product_line_code = product_master_active_only.product_line_code
         LEFT JOIN workflow ON workflow.sku_number = lasts.last_sku
         LEFT JOIN loc_country ON losses.location_uid = loc_country.location_uid
         LEFT JOIN sku_asp ON loc_country.country = sku_asp.country AND lasts.last_sku = sku_asp.sku_number
WHERE qty_15m <> 0;


--endpoint query for leadshop lost business with and without bag option
--bag option commented out; not useful for this exercise
--revised sku input for Gibco
--NOTE: if interested in covid flag for sku exclusion, lsg_product.product_master.covid_sku_flag can be utilized
DROP TABLE IF EXISTS gibco;
create temp table gibco as
WITH /* bag as (SELECT distinct product_line_code
             FROM lsg_territory.master_sales_hierarchy
                      INNER JOIN lsg_territory.master_product_bag
                                 ON master_sales_hierarchy.am_type = master_product_bag.position_type
             WHERE %(am_code) s in (sd_territory_code, flex_territory_code, rm_code, am_code)),
        */
    gibco as (SELECT distinct sku_number,
                              product_name
              FROM lsg_product.product_entitlement
              WHERE brand ilike '%gibco%')
SELECT distinct cl.nsgn_number,
                cl.nsgn_name,
                cl.sgn_number,
                cl.sgn_name,
                sl.location_uid,
                packsize_uid,
                sl.sku_number,
                sku_name,
                sku_size,
                sl.product_line_code,
                sl.product_line_name,
                sl.product_line_group_code,
                sl.product_line_group_name,
                global_region_code,
                global_region_name,
                portfolio,
                keycodes,
                workflow_family,
                keycode_names,
                last_revenue,
                last_quantity,
                last_order,
                recurrence,
                rev_15m,
                rev_3m,
                rev_1m,
                rev_precovid,
                rev_12m,
                avg_quart_precovid,
                avg_quart_15m,
                avg_quart_12m,
                opportunity,
                opp_rev_precovid,
                ntile(6) over (order by score desc) as rank,
                (case
                     when rank = 1 then 'A'
                     when rank = 2 then 'B'
                     when rank = 3 then 'C'
                     else 'D' end)                  as score,
                price_realization,
                customer_classification,
                is_new,
                classification_12m,
                sl.created_at
FROM stage_losses sl
         INNER JOIN lsg_laboratory.combined_location cl on cl.location_uid = sl.location_uid
         INNER JOIN gibco g on g.sku_number = sl.sku_number
ORDER BY score, price_realization desc;


--test view output
select *
from gibco;
----------------------------QUICK STATS----------------------------------------
-- quick stats below are from 20221221
--how many sgn's are there in this gibco winback?
-- 2456
select count(distinct sgn_number)
from gibco;

--how many location_uid are there in this gibco winback?
-- 5358
select count(distinct location_uid)
from gibco;

--how many of the 1995 gibco skus are being purchased?
-- 973
select count(distinct sku_number)
from gibco;

--which skus purchased most?
-- 17504044, 303 times
select sku_number, count(*) as num_buy
from gibco
group by sku_number
order by num_buy desc;


-- what is the sum of precovid revenue opportunity?
-- 23,796,759
select sum(opp_rev_precovid)
from gibco;

-- what is the sum of 15m lookback revenue opportunity?
-- 49,268,520
select sum(opportunity) from gibco;


