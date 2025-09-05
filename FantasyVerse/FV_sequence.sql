WITH params AS (
  SELECT DATE '2024-11-14' AS fv_launch_dt
),

-- Map products to simple flags
flags AS (
  SELECT
    product_id,
    (SUM(CASE WHEN collection = 'FantasyVerse' THEN 1 ELSE 0 END) > 0) AS is_fv
  FROM shop.product_collections
  GROUP BY 1
),


-- Order lines with money normalized to currency units
line AS (
  SELECT
    od.order_id,
    od.user_id,
    od.order_date::timestamp AS order_ts,
    DATE_TRUNC('day', od.order_date)::date AS order_date,
    od.product_id,
    p.name AS product_name,
    od.units,
    (od.revenue_ex_vat::numeric / 100.0) AS price, -- purchase price (ex VAT)
    (od.margin::numeric / 100.0) AS margin_amt, -- margin amount
    CASE
        WHEN f.is_fv IS TRUE THEN 'FantasyVerse'
        WHEN p.product_type IS NOT NULL THEN p.product_type
        WHEN f.is_fv IS FALSE THEN 'Other'
        ELSE 'Unknown'
        END AS segment_final
  FROM shop.order_details od
  LEFT JOIN flags f USING (product_id)
  LEFT JOIN shop.products p USING (product_id)
  WHERE od.order_date >= DATE '2024-11-14' AND od.order_date < DATE '2025-09-01'
  AND od.revenue_ex_vat >0
  AND od.margin > 0

),


line_agg AS(
SELECT
    *,
    CASE WHEN segment_final = 'FantasyVerse' THEN 1 ELSE 0 END AS is_fv,
    CASE WHEN segment_final = 'game' THEN 1 ELSE 0 END AS is_game
FROM line),

-- Order-level rollups for "has FV" / "has Game"
ord AS (
  SELECT
    order_id,
    user_id,
    MIN(order_ts) AS order_ts,
    BOOL_OR(is_fv) AS has_fv,
    BOOL_OR(is_game) AS has_game
  FROM line_agg
  GROUP BY 1,2
),

-- First-ever order per user (across all time)
first_ord AS (
  SELECT
    o.user_id,
    o.order_id AS first_order_id,
    o.order_ts AS first_order_ts,
    o.has_fv AS first_has_fv,
    o.has_game AS first_has_game,
    ROW_NUMBER() OVER (PARTITION BY o.user_id ORDER BY o.order_ts) AS rn
  FROM ord o
),
first_clean AS (
  SELECT * FROM first_ord WHERE rn = 1
),

-- Users whose first order was FV (on/after launch)
fv_first_users AS (
  SELECT
    fo.user_id,
    'FV_FIRST'::varchar AS cohort_type
  FROM first_clean fo
  JOIN params p ON fo.first_order_ts::date >= p.fv_launch_dt
  WHERE fo.first_has_fv = TRUE
),

-- Users whose first order was Game-only and later bought FV (on/after launch)
game_then_fv_users AS (
  SELECT DISTINCT
    fo.user_id,
    'GAME_THEN_FV'::varchar AS cohort_type
  FROM first_clean fo
  JOIN ord o2
    ON o2.user_id = fo.user_id
   AND o2.has_fv = TRUE
  JOIN params p ON o2.order_ts::date >= p.fv_launch_dt
  WHERE fo.first_has_game = TRUE
    AND fo.first_has_fv   = FALSE
),

eligible_users AS (
  SELECT * FROM fv_first_users
  UNION ALL
  SELECT * FROM game_then_fv_users
),

-- Final: all orders (line-level) for eligible users
final_lines AS (
  SELECT
    eu.cohort_type,
    l.user_id,
    l.order_id,
    l.order_date,
    l.product_id,
    l.product_name,
    l.units,
    l.price,
    l.margin_amt,
    CASE WHEN l.price > 0 THEN l.margin_amt / l.price ELSE NULL END AS margin_pct,
    l.is_fv,
    l.is_game,
    ROW_NUMBER() OVER (PARTITION BY l.user_id ORDER BY l.order_ts) AS order_seq_num
  FROM line_agg l
  JOIN eligible_users eu USING (user_id)
),


orders AS (
  SELECT
    user_id,
    order_id,
    MIN(order_date)    AS order_date,
    MIN(order_seq_num) AS order_seq_num,
    MAX(CASE WHEN is_fv   = 1 THEN 1 ELSE 0 END) AS has_fv,
    MAX(CASE WHEN is_game = 1 THEN 1 ELSE 0 END) AS has_game
  FROM final_lines
  GROUP BY 1,2
),

-- SAME-OR-NEXT Game-containing order after an FV order  (CHANGED: >=)
next_game AS (
  SELECT * FROM (
    SELECT
      b.user_id,
      b.order_id        AS fv_order_id,
      b.order_seq_num   AS fv_seq,
      b.order_date      AS fv_date,
      ng.order_id       AS next_game_order_id,
      ng.order_seq_num  AS next_game_seq,
      ng.order_date     AS next_game_date,
      ROW_NUMBER() OVER (
        PARTITION BY b.user_id, b.order_id
        ORDER BY ng.order_seq_num
      ) AS rn
    FROM orders b
    JOIN orders ng
      ON ng.user_id = b.user_id
     AND ng.order_seq_num >= b.order_seq_num   -- CHANGED
     AND ng.has_game = 1
    WHERE b.has_fv = 1
  ) s
  WHERE rn = 1
),

-- SAME-OR-NEXT FV-containing order after a Game order  (CHANGED: >=)
next_fv AS (
  SELECT * FROM (
    SELECT
      b.user_id,
      b.order_id        AS game_order_id,
      b.order_seq_num   AS game_seq,
      b.order_date      AS game_date,
      nf.order_id       AS next_fv_order_id,
      nf.order_seq_num  AS next_fv_seq,
      nf.order_date     AS next_fv_date,
      ROW_NUMBER() OVER (
        PARTITION BY b.user_id, b.order_id
        ORDER BY nf.order_seq_num
      ) AS rn
    FROM orders b
    JOIN orders nf
      ON nf.user_id = b.user_id
     AND nf.order_seq_num >= b.order_seq_num   -- CHANGED
     AND nf.has_fv = 1
    WHERE b.has_game = 1
  ) s
  WHERE rn = 1
),

fv_lines AS (
  SELECT DISTINCT order_id, product_name
  FROM final_lines
  WHERE is_fv = 1
),
game_lines AS (
  SELECT DISTINCT order_id, product_name
  FROM final_lines
  WHERE is_game = 1
),

edges_fv2game AS (
  SELECT
    n.user_id,
    n.fv_order_id        AS source_order_id,
    n.next_game_order_id AS target_order_id,
    fl.product_name      AS source_product,
    gl.product_name      AS target_product,
    (n.next_game_date::date - n.fv_date::date) AS days_to_next,
    'FV→Game'            AS direction
  FROM next_game n
  JOIN fv_lines   fl ON fl.order_id = n.fv_order_id
  JOIN game_lines gl ON gl.order_id = n.next_game_order_id
),

edges_game2fv AS (
  SELECT
    n.user_id,
    n.game_order_id      AS source_order_id,
    n.next_fv_order_id   AS target_order_id,
    gl.product_name      AS source_product,
    fl.product_name      AS target_product,
    (n.next_fv_date::date - n.game_date::date) AS days_to_next,
    'Game→FV'            AS direction
  FROM next_fv n
  JOIN game_lines gl ON gl.order_id = n.game_order_id
  JOIN fv_lines   fl ON fl.order_id = n.next_fv_order_id
),

all_edges AS (
  SELECT * FROM edges_fv2game
  UNION ALL
  SELECT * FROM edges_game2fv
),

agg AS (
  SELECT
    direction,
    source_product,
    target_product,
    COUNT(DISTINCT user_id)            AS users,
    AVG(days_to_next)::numeric(10,2)   AS avg_days_to_next
  FROM all_edges
  WHERE days_to_next <= 30  -- optional window; if you add here, also add in pairs_sub
  GROUP BY 1,2,3
),
pairs_sub AS (  -- distinct order-to-order transitions
  SELECT DISTINCT
    direction, source_product, target_product,
    user_id, source_order_id, target_order_id
  FROM all_edges
  WHERE days_to_next <= 30  -- keep in sync with agg if you use the window
),
pairs AS (
  SELECT
    direction, source_product, target_product,
    COUNT(*) AS order_pairs
  FROM pairs_sub
  GROUP BY 1,2,3
),

joined AS (
  SELECT
    a.direction,
    a.source_product,
    a.target_product,
    a.users,
    COALESCE(p.order_pairs, 0) AS order_pairs,
    a.avg_days_to_next
  FROM agg a
  LEFT JOIN pairs p USING (direction, source_product, target_product)
),

ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY direction
      ORDER BY users DESC, order_pairs DESC
    ) AS rk
  FROM joined
)
SELECT
  direction,
  source_product,
  target_product,
  users,
  order_pairs,
  avg_days_to_next
FROM ranked
WHERE rk <= 50
ORDER BY direction, rk;
