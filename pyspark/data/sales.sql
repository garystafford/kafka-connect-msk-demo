-- SEED SALES
SELECT p.payment_id,
       p.customer_id,
       (CASE
            WHEN p.amount = 0.00 THEN 2.99
            ELSE p.amount + 3.00
           END)                                AS amount,
       (p.payment_date + interval '1565 days') AS payment_date,
       ci.city,
       a.district,
       co.country
FROM payment AS p
         INNER JOIN customer AS cu ON p.customer_id = cu.customer_id
         INNER JOIN address AS a ON cu.address_id = a.address_id
         INNER JOIN city AS ci ON a.city_id = ci.city_id
         INNER JOIN country AS co ON ci.country_id = co.country_id
ORDER BY payment_date ASC
LIMIT 250;
--
-- INCREMENTAL SALES (600 records)
SELECT p.payment_id,
       p.customer_id,
       (CASE
            WHEN p.amount = 0.00 THEN 2.99
            ELSE p.amount + 3.00
           END)                                AS amount,
       (p.payment_date + interval '1565 days') AS payment_date,
       ci.city,
       a.district,
       co.country
FROM payment AS p
         INNER JOIN customer AS cu ON p.customer_id = cu.customer_id
         INNER JOIN address AS a ON cu.address_id = a.address_id
         INNER JOIN city AS ci ON a.city_id = ci.city_id
         INNER JOIN country AS co ON ci.country_id = co.country_id
ORDER BY payment_date ASC
OFFSET 250 LIMIT 600;
--
-- INCREMENTAL SALES (1800 records)
SELECT p.payment_id,
       p.customer_id,
       (CASE
            WHEN p.amount = 0.00 THEN 2.99
            ELSE p.amount + 3.00
           END)                                AS amount,
       (p.payment_date + interval '1565 days') AS payment_date,
       ci.city,
       a.district,
       co.country
FROM payment AS p
         INNER JOIN customer AS cu ON p.customer_id = cu.customer_id
         INNER JOIN address AS a ON cu.address_id = a.address_id
         INNER JOIN city AS ci ON a.city_id = ci.city_id
         INNER JOIN country AS co ON ci.country_id = co.country_id
ORDER BY payment_date ASC
OFFSET 250 LIMIT 1800;