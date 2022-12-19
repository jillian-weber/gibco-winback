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
    rev_15m           numeric(23, 2),
    rev_3m            numeric(23, 2),
    rev_1m            numeric(23, 2),
    rev_precovid      numeric(23, 2),
    rev_12m           numeric(23, 2),
    avg_quart_precovid numeric(23, 2),
    opportunity       numeric(23, 2),
    opp_rev_precovid  numeric(23, 2),
    revenue           integer,
    inertia           integer,
    frequency         integer,
    recency           integer,
    repeat            integer,
    score             numeric(23, 2),
    price_realization numeric(23, 2),
    -- customer_classification is for "lost", "declining", or "new" classification
    customer_classification varchar(50),
    created_at        timestamptz default (current_timestamp)
);



INSERT INTO stage_losses (location_uid, packsize_uid, sku_number, sku_name, sku_size, product_line_code,
                          product_line_name, product_line_group_code, product_line_group_name, global_region_code, global_region_name, portfolio, keycodes, workflow_family,
                          keycode_names,
                          last_revenue, last_quantity, last_order, recurrence, rev_15m, rev_3m, rev_1m,
                          rev_precovid, rev_12m, avg_quart_precovid,
                          opportunity, opp_rev_precovid, revenue, inertia, frequency, recency, repeat,
                          score, customer_classification, price_realization)

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
-- comparing past 12 months to 2019 precovid to classify as lost or declining
-- logic is that if the revenue is 0 in the past 12months then they are "lost",
-- if the revenue is greater than 0 then "declining" (here i wrote it as if precovid is greater than past year)
-- if the minimum ship date year is greater than 2019 then the customer is "new"
                       CASE
                           WHEN rev_12m = 0
                                THEN 'LOST'
                           WHEN rev_precovid > rev_12m
                                THEN 'DECLINING'
                           -- when min ship date year > 2019 then 'NEW'
                           WHEN min(ship_date) > '2019-12-31'::DATE
                                THEN 'NEW'
                           WHEN rev_12m > rev_precovid
                                THEN 'INCREASING'
                           ELSE 'NULL'
                       END   AS customer_classification
                FROM lsg_sales.tf_transactions_mapped
                         INNER JOIN lsg_laboratory.combined_delivery
                                    ON combined_delivery.del_uid = tf_transactions_mapped.del_uid
                         INNER JOIN lsg_product.master_sku ON tf_transactions_mapped.sku_number = master_sku.sku_number
               -- WHERE ship_date > (current_date - INTERVAL '15 months')
                GROUP BY location_uid,
                         packsize_uid,
                         master_sku.sku_number
                HAVING (((rev_precovid / 4.0) > rev_3m) OR  ((rev_15m / 5.0) > rev_3m))
                   AND (((rev_3m - (rev_precovid / 4.0)) < -1000) OR ((rev_3m - (rev_15m / 5.0)) < -1000))
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
                (rev_precovid / 4)                                                                                  AS avg_quart_precovid,
                abs(rev_3m - (rev_15m / 5.0))                                                                   AS opp_rev,
                abs(rev_3m - (rev_precovid / 4.0))                                                                  AS opp_rev_precovid,
                labs.rev_rank                                                                                       AS revenue,
                labs.growth_rank                                                                                    AS inertia,
                skus.orders_rank                                                                                    AS frequency,
                skus.recency_rank                                                                                   AS recency,
                (CASE WHEN datediff_rank IS NULL THEN 0 ELSE datediff_rank END)                                     AS repeat,
                sqrt((revenue * revenue + inertia * inertia + frequency * frequency + recency * recency +
                      repeat * repeat) /5)                                                                          AS score, --RMS, not super necessary here, but fun to play with;
                customer_classification,
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
                sl.created_at
FROM stage_losses sl
         INNER JOIN lsg_laboratory.combined_location cl on cl.location_uid = sl.location_uid
         INNER JOIN gibco g on g.sku_number = sl.sku_number
ORDER BY score, price_realization desc;


--test view output
select *
from gibco;
----------------------------QUICK STATS----------------------------------------
--how many sgn's are there in this gibco winback?
--2311 sgn's
select count(distinct sgn_number)
from gibco;
-- 12/5 2261

--how many location_uid are there in this gibco winback?
--4985
select count(distinct location_uid)
from gibco;
--12/5 4866

--how many of the 1995 gibco skus are being purchased?
--926 distinct skus purchased within the last 15 months
select count(distinct sku_number)
from gibco;
--12/5 887

--which skus purchased most?
--261 times is max
select sku_number, count(*) as num_buy
from gibco
group by sku_number
order by num_buy desc;

-- 12/5 A1435102 300 times is max


select sum(opp_rev_precovid)
from gibco;

select sum(opportunity)
from gibco;
where customer_classification = 'DECLINING';

select sum(opportunity) from gibco;
-- before region: 48518501

select count(*) from gibco;
--before region: 9495
-- after region: 9536

-- 9177
-- 3802
-- 3802

--60830
--11185

select count(location_uid) from gibco where customer_classification= 'DECLINING';
--997
-- 2495

-- 22816
select count(location_uid) from gibco where customer_classification= 'INCREASING';
--2092
-- 618

--21497
select count(location_uid) from gibco where customer_classification= 'LOST';
--1122
--665

--11446

select count(location_uid) from gibco where customer_classification= 'NEW';
-- 5023
select count(distinct location_uid) from lsg_laboratory.combined_location;