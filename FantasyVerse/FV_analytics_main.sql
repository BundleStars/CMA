WITH params AS (
  SELECT 30::int AS seq_window      -- sequencing window (days)
),

flags AS (
  SELECT
    product_id,
    (SUM(CASE WHEN collection = 'FantasyVerse' THEN 1 ELSE 0 END) > 0) AS is_fv
  FROM shop.product_collections
  GROUP BY 1
),

prod_types AS(
SELECT
    DISTINCT
    product_id,
    product_type
FROM
    shop.products
),

-- 2) Line level with 3-way segment (FV / Other / Unknown)
line AS (SELECT od.order_id,
                od.user_id,
                od.order_date::date                      AS order_date,
                DATE_TRUNC('month', od.order_date)::date AS order_month,
                od.product_id,
                od.units,
                od.revenue_ex_vat::numeric / 100         AS revenue,
                od.margin::numeric / 100                 AS margin,
--                 pt.product_type,
--                 CASE
--                     WHEN f.is_fv IS TRUE THEN 'FantasyVerse'
--                     WHEN f.is_fv IS FALSE THEN 'Other'
--                     ELSE 'Unknown'
--                     END AS segment,
                CASE
                    WHEN f.is_fv IS TRUE THEN 'FantasyVerse'
                    WHEN pt.product_type IS NOT NULL THEN pt.product_type
                    WHEN f.is_fv IS FALSE THEN 'Other'
                    ELSE 'Unknown'
                    END AS segment_final
         FROM shop.order_details od
                  LEFT JOIN flags f USING (product_id)
                  LEFT JOIN prod_types pt USING (product_id)-- include unmapped
         WHERE od.status = 'COMPLETE'
           AND od.order_date >= DATE '2024-11-14'
           AND od.order_date < DATE '2025-08-25'
),

line_agg AS(
SELECT
    *,
    CASE
    WHEN segment_final = 'FantasyVerse' THEN 'FantasyVerse'
    WHEN LOWER(segment_final) IN ('gift-card','video','comic','voucher','book','audio','software') THEN 'Other'
    ELSE segment_final
  END AS segment_group
FROM line),

-- 3) Order-level rollup with aligned flags
ord AS (
  SELECT
    order_id,
    user_id,
    order_date,
    DATE_TRUNC('month', order_date)::date AS order_month,
    SUM(revenue)::numeric AS order_revenue_total,
    SUM(margin)::numeric  AS order_margin_total,
    SUM(units)            AS units,
    MAX(CASE WHEN segment_group = 'FantasyVerse' THEN 1 ELSE 0 END) AS has_fv,
    MAX(CASE WHEN segment_group = 'game' THEN 1 ELSE 0 END) AS has_game,
    MAX(CASE WHEN segment_group = 'dlc' THEN 1 ELSE 0 END) AS has_dlc,
    MAX(CASE WHEN segment_group = 'Other' THEN 1 ELSE 0 END) AS has_other,

    CASE
      WHEN (MAX(CASE WHEN segment_group = 'FantasyVerse' THEN 1 ELSE 0 END)
          + MAX(CASE WHEN segment_group = 'game' THEN 1 ELSE 0 END)
          + MAX(CASE WHEN segment_group = 'dlc' THEN 1 ELSE 0 END)
          + MAX(CASE WHEN segment_group = 'Other' THEN 1 ELSE 0 END)) > 1 THEN 'Mixed'
      WHEN MAX(CASE WHEN segment_group = 'FantasyVerse' THEN 1 ELSE 0 END) = 1 THEN 'FantasyVerse'
      WHEN MAX(CASE WHEN  segment_group = 'game' THEN 1 ELSE 0 END) = 1 THEN 'game'
      WHEN MAX(CASE WHEN segment_group = 'dlc' THEN 1 ELSE 0 END) = 1 THEN 'dlc'
      WHEN MAX(CASE WHEN segment_group = 'Other' THEN 1 ELSE 0 END) = 1 THEN 'Other'
      ELSE 'Unknown'
    END AS order_type
  FROM line_agg
  GROUP BY 1,2,3,4
),

-- 4) Gap between consecutive orders (per user)
ord_gap AS (
  SELECT
    user_id,
    order_id,
    DATEDIFF(day,
             LAG(order_date) OVER (PARTITION BY user_id ORDER BY order_date),
             order_date)::numeric AS days_since_prev_order
  FROM ord
),

-- 5) Other-only → FV-only within N days
seq_g2fv_pairs AS (
  SELECT
    o1.user_id,
    o1.order_id,
    o2.order_date AS second_ts,
    ROW_NUMBER() OVER (PARTITION BY o1.user_id, o1.order_id ORDER BY o2.order_date) AS rn
  FROM ord o1
  JOIN ord o2
    ON o2.user_id = o1.user_id
   AND o1.order_date < o2.order_date
   AND o2.order_date <= o1.order_date + (SELECT seq_window FROM params)
  WHERE
    -- seed: pure game
    o1.has_game = 1 AND o1.has_fv = 0 AND o1.has_dlc = 0 AND o1.has_other = 0
    -- target: any FV present (FV-only or Mixed)
    AND o2.has_fv = 1
),
seq_g2fv AS (
  SELECT user_id, order_id, second_ts, COUNT(*) AS seq_g2fv_30d_count
  FROM seq_g2fv_pairs
  WHERE rn = 1
  GROUP BY 1,2,3
),

-- 6) FV-only → Other-only within N days
seq_fv2g_pairs AS (
  SELECT
    o1.user_id,
    o1.order_id,
    o2.order_date AS second_ts,
    ROW_NUMBER() OVER (PARTITION BY o1.user_id, o1.order_id ORDER BY o2.order_date) AS rn
  FROM ord o1
  JOIN ord o2
    ON o2.user_id = o1.user_id
   AND o1.order_date < o2.order_date
   AND o2.order_date <= o1.order_date + (SELECT seq_window FROM params)
  WHERE
    -- seed: pure FV
    o1.has_fv = 1 AND o1.has_other = 0 AND o1.has_dlc = 0 AND o1.has_game = 0
    -- target: any game present (Other-only or Mixed)
    AND o2.has_game = 1
),
seq_fv2g AS (
  SELECT user_id, order_id, second_ts, COUNT(*) AS seq_fv2g_30d_count
  FROM seq_fv2g_pairs
  WHERE rn = 1
  GROUP BY 1,2,3
),

-- 7) Attach gaps and flags to each order row
user_month AS (
  SELECT
    o.user_id,
    o.order_id,
    o.order_month,
    o.order_revenue_total,
    o.order_margin_total,
    o.has_fv,
    o.has_other,
    o.has_dlc,
    o.has_game,
    o.order_type,
    g.days_since_prev_order
  FROM ord o
  LEFT JOIN ord_gap g USING (user_id, order_id)
),

-- 8) Add sequencing counts
user_month_plus AS (
  SELECT
    um.*,
    COALESCE(s1.seq_g2fv_30d_count, 0) AS seq_g2fv_30d_count,
    COALESCE(s2.seq_fv2g_30d_count, 0) AS seq_fv2g_30d_count
  FROM user_month um
  LEFT JOIN seq_g2fv s1 ON s1.user_id = um.user_id AND s1.order_id = um.order_id
  LEFT JOIN seq_fv2g s2 ON s2.user_id = um.user_id AND s2.order_id = um.order_id
),

-- 9) Aggregate to user-month with aligned flags
user_month_agg AS (
  SELECT
    order_id,
    user_id,
    order_month,
    order_type,
    COUNT(*) AS orders_count,
    SUM(order_revenue_total) AS revenue_total,
    SUM(order_margin_total) AS margin_total,
    AVG(days_since_prev_order)::numeric AS avg_days_between_orders,
    SUM(seq_g2fv_30d_count) AS seq_g2fv_30d_count,
    SUM(seq_fv2g_30d_count) AS seq_fv2g_30d_count,
    CASE WHEN SUM(has_fv) > 0 THEN 1 ELSE 0 END AS has_fv,
    CASE WHEN SUM(has_other) > 0 THEN 1 ELSE 0 END AS has_other,
    CASE WHEN SUM(has_dlc)> 0 THEN 1 ELSE 0 END AS has_dlc,
    CASE WHEN SUM(has_game)> 0 THEN 1 ELSE 0 END AS has_game
  FROM user_month_plus
  GROUP BY 1,2,3,4
)

-- 10) Final
SELECT
  uma.*,
  MIN(uma.order_month) OVER (PARTITION BY uma.user_id) AS first_order_month,
  DATEDIFF(
    month,
    MIN(uma.order_month) OVER (PARTITION BY uma.user_id),
    uma.order_month
  ) AS months_since_first
FROM user_month_agg uma
ORDER BY uma.order_month DESC;
