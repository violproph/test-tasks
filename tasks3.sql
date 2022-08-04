WITH id_cntr AS
(
       SELECT user_id ,
              country_code ,
              registration_time
       FROM   (
                       SELECT   user_id ,
                                country_code ,
                                Row_number() OVER(partition BY user_id ORDER BY registration_time) AS user_reg_num ,
                                registration_time
                       FROM     users) t1
       WHERE  user_reg_num = 1) ,
-- Дата и время регистрации
-- Страна
frst_dep AS
(
       SELECT user_id ,
              operation_time ,
              operation_amount_usd
       FROM   (
                       SELECT   user_id ,
                                Row_number() OVER(partition BY user_id ORDER BY operation_time) AS user_depos_num ,
                                operation_amount_usd ,
                                operation_time
                       FROM     balance
                       WHERE    operation_type = 'deposit') t2
       WHERE  user_depos_num = 1),
-- Дата и время первого депозита
--- Сумма первого депозита
frst_prft AS
(
       SELECT user_id ,
              open_time ,
              profit_usd
       FROM   (
                       SELECT   user_id ,
                                Row_number() OVER(partition BY user_id ORDER BY open_time, close_time) AS user_oper_num ,
                                open_time ,
                                profit_usd
                       FROM     orders) t3
       WHERE  user_oper_num = 1),
-- Дата и время первой сделки (если есть)
--- Прибыль/убыток первой сделки
ttl_usd AS
(
           SELECT     t4.user_id ,
                      Round(Sum(depos_usd)::numeric,2) AS total_dep_reg ,
                      Round(Sum(wd_usd)::   numeric,2) AS total_wd_reg
           FROM       (
                             SELECT * ,
                                    CASE
                                           WHEN operation_type = 'deposit' THEN operation_amount_usd
                                    END AS depos_usd ,
                                    CASE
                                           WHEN operation_type = 'withdrawal' THEN operation_amount_usd
                                    END AS wd_usd
                             FROM   balance) t4
           INNER JOIN users
           ON         t4.user_id = users.user_id
           AND        registration_time + interval '30 day' >= operation_time
           GROUP BY   t4.user_id), prft_30 AS
(
                SELECT DISTINCT orders.user_id ,
                                round(sum(profit_usd)::numeric,2) AS total_prof
                FROM            orders
                INNER JOIN      users
                ON              orders.user_id = users.user_id
                AND             registration_time + interval '30 day' >= close_time
                GROUP BY        orders.user_id), prft_all AS
(
                SELECT DISTINCT user_id ,
                                round(sum(profit_usd)::numeric,2) AS prof_life_time
                FROM            orders
                GROUP BY        user_id)
SELECT     id_cntr.user_id -- ID пользователя
           ,
           id_cntr.country_code -- Страна
           ,
           id_cntr.registration_time -- Дата и время регистрации
           ,
           operation_time -- Дата и время первого депозита
           ,
           open_time -- Дата и время первой сделки (если есть)
           ,
           operation_amount_usd -- Сумма первого депозита
           ,
           profit_usd -- Прибыль/убыток первой сделки
           ,
           total_dep_reg -- Общий депозит за первые 30 дней с момента регистрации
           ,
           total_wd_reg -- Суммарный вывод за первые 30 дней после регистрации
           ,
           total_prof -- Общая прибыль/убыток за первые 30 дней после регистрации
           ,
           prof_life_time --- ОБЩАЯ прибыль/убыток за время жизни пользователя
FROM       id_cntr
INNER JOIN frst_dep
ON         id_cntr.user_id = frst_dep.user_id
INNER JOIN frst_prft
ON         frst_dep.user_id = frst_prft.user_id
INNER JOIN ttl_usd
ON         frst_prft.user_id = ttl_usd.user_id
INNER JOIN prft_30