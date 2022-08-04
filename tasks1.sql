WITH type_opr
     AS (SELECT user_id,
                CASE
                  WHEN operation_type = 'deposit' THEN operation_amount_usd
                  ELSE NULL
                END AS deposit,
                CASE
                  WHEN operation_type = 'withdrawal' THEN operation_amount_usd
                  ELSE NULL
                END AS withdrawal
         FROM   balance)
SELECT cnt_reg.country_code,
       Count(DISTINCT cnt_reg.user_id) AS cnt_user  -- Общее количество пользователей из этой страны
       ,
       Count(DISTINCT type_opr.user_id) AS cnt_depos_user -- Количество пользователей, сделавших хотя бы один депозит
       ,
       Round(Avg(deposit) :: NUMERIC, 2) AS avg_user_depos -- Средняя сумма депозита по стране
       ,
       Round(Avg(withdrawal) :: NUMERIC, 2) AS avg_user_wd -- Средняя сумма вывода по стране
FROM   (SELECT *,
               Row_number()
                 over(
                   PARTITION BY user_id
                   ORDER BY registration_time) AS user_reg_num
        FROM   users) AS cnt_reg
       left join type_opr
              ON cnt_reg.user_id = type_opr.user_id
WHERE  user_reg_num = 1
GROUP  BY cnt_reg.country_code
ORDER  BY cnt_user DESC 