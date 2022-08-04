WITH w2 AS
(
                SELECT DISTINCT user_id ,
                                First_value(symbol) OVER (partition BY user_id ORDER BY sym_prof DESC) AS pop_sym -- Самый популярный торговый инструмент (символ)
                                ,
                                First_value(symbol) OVER (partition BY user_id ORDER BY group_profit_up DESC) sym_up -- Символ с самым высоким уровнем прибыли
                                ,
                                Last_value(symbol) OVER (partition BY user_id ORDER BY group_profit_down DESC) sym_down -- Символ с самым высоким уровнем проигрыша
                FROM            (
                                         SELECT   * ,
                                                  CASE
                                                           WHEN profit_usd > 0 THEN profit_usd
                                                  END AS group_profit_up ,
                                                  CASE
                                                           WHEN profit_usd < 0 THEN profit_usd
                                                  END                                                                AS group_profit_down ,
                                                  Row_number() OVER(partition BY user_id, symbol ORDER BY open_time) AS sym_prof
                                         FROM     orders) qwer), w3 AS
(
          SELECT    w1.user_id -- ID этого пользователя
                    ,
                    country_code -- Его код страны
                    ,
                    Round((Sum(profit_usd))::numeric,2) AS sum_prof -- Его прибыль
                    ,
                    Round(Sum(Abs(profit_usd))::numeric,2) AS abs_sum_prof -- Общая сумма сделок
                    ,
                    Count(w1.group_profit) AS up_profit -- Количество прибыльных сделок
          FROM      (
                           SELECT * ,
                                  CASE
                                         WHEN profit_usd > 0 THEN 1
                                  END AS group_profit
                           FROM   orders) w1
          LEFT JOIN users
          ON        w1.user_id = users.user_id
          GROUP BY  w1.user_id ,
                    country_code)
SELECT    *
FROM      w3
LEFT JOIN w2
ON        w2.user_id = w3.user_id
ORDER BY  sum_prof DESC limit 1