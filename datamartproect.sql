-- создание схемы 

CREATE SCHEMA IF NOT EXISTS DM;

-- создание таблиц

CREATE TABLE IF NOT EXISTS dm.dm_account_turnover_f (
on_date date NOT NULL,
account_rk NUMERIC NOT NULL,
credit_amount NUMERIC(23, 8),
credit_amount_rub numeric(23, 8),
debet_amount numeric(23, 8),
debet_amount_rub numeric(23, 8)
);

CREATE TABLE IF NOT EXISTS dm.dm_account_balance_f (
on_date date NOT NULL,
account_rk NUMERIC NOT NULL,
balance_out numeric(23, 8),
balance_out_rub numeric(23, 8)
);

-- процедуры рассчета витрин

CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f (i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time timestamp := CURRENT_TIMESTAMP;
BEGIN
    INSERT INTO logs.log (timestamp, status, message, obj_num, task_name)
    VALUES (CURRENT_TIMESTAMP, 'ИНФО', 'Начало расчета витрины dm.dm_account_turnover_f', NULL, 'ds.fill_account_turnover_f');
    
    DELETE FROM dm.dm_account_turnover_f WHERE on_date = i_OnDate;
    
    INSERT INTO dm.dm_account_turnover_f (
        on_date,
        account_rk,
        credit_amount,
        credit_amount_rub,
        debet_amount,
        debet_amount_rub
    )
    SELECT 
        i_OnDate AS on_date,
        t.account_rk,
        SUM(t.credit_amount) AS credit_amount,
        SUM(t.credit_amount * COALESCE(er.reduced_cource, 1)) AS credit_amount_rub,
        SUM(t.debet_amount) AS debet_amount,
        SUM(t.debet_amount * COALESCE(er.reduced_cource, 1)) AS debet_amount_rub
    FROM (
        SELECT
            credit_account_rk AS account_rk,
            SUM(credit_amount) AS credit_amount,
            0 AS debet_amount
        FROM DS.FT_POSTING_F
        WHERE oper_date = i_OnDate
        GROUP BY credit_account_rk
        UNION ALL
        SELECT
            debet_account_rk AS account_rk,
            0 AS credit_amount,
            SUM(debet_amount) AS debet_amount
        FROM DS.FT_POSTING_F
        WHERE oper_date = i_OnDate
        GROUP BY debet_account_rk
    ) t
    LEFT JOIN DS.MD_ACCOUNT_D acc
        ON t.account_rk = acc.account_rk
        AND i_OnDate BETWEEN acc.data_actual_date AND COALESCE(acc.data_actual_end_date, '9999-12-31'::DATE)
    LEFT JOIN DS.MD_EXCHANGE_RATE_D er
        ON acc.currency_rk = er.currency_rk
        AND i_OnDate BETWEEN er.data_actual_date AND COALESCE(er.data_actual_end_date, '9999-12-31'::DATE)
    GROUP BY t.account_rk
    HAVING SUM(t.credit_amount) > 0 OR SUM(t.debet_amount) > 0;

    INSERT INTO logs.log (timestamp, status, message, obj_num, task_name)
    VALUES (CURRENT_TIMESTAMP, 'ИНФО', 'Окончание расчета витрины dm.dm_account_turnover_f за ' || 
            TO_CHAR(i_OnDate, 'YYYY-MM-DD') || '. Время выполнения: ' || 
            (CURRENT_TIMESTAMP - v_start_time)::text, NULL, 'ds.fill_account_turnover_f');

EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO logs.log (timestamp, status, message, obj_num, task_name)
        VALUES (CURRENT_TIMESTAMP, 'ОШИБКА', 'Ошибка при расчете витрины dm.dm_account_turnover_f за ' || 
                TO_CHAR(i_OnDate, 'YYYY-MM-DD') || ': ' || SQLERRM, NULL, 'ds.fill_account_turnover_f');
        RAISE;
END;
$$;

-- цикл заполнения таблицы dm.dm_account_turnover_f данными за январь 

DO $$
BEGIN
    FOR i IN 1..31 LOOP
        CALL ds.fill_account_turnover_f(TO_DATE('2018-01-' || i, 'YYYY-MM-DD'));
    END LOOP;
END $$;

-- инициализация витрины dm.dm_account_balance_f с данными за 31.12.2017

DO $$
BEGIN
	INSERT INTO logs.log (timestamp, status, message, obj_num, task_name)
    VALUES (CURRENT_TIMESTAMP, 'ИНФО', 'Инициализация витрины dm.dm_account_balance_f', NULL, 'dm.dm_account_balance_f');
	
	INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
	SELECT
        b.on_date,
        b.account_rk,
        b.balance_out,
        b.balance_out * COALESCE(er.reduced_cource, 1) AS balance_out_rub
    FROM DS.FT_BALANCE_F b
    LEFT JOIN DS.MD_EXCHANGE_RATE_D er
        ON b.currency_rk = er.currency_rk
        AND '2017-12-31'::DATE BETWEEN er.data_actual_date 
        AND COALESCE(er.data_actual_end_date, '9999-12-31'::DATE)
    WHERE b.on_date = '2017-12-31'::DATE;
	
	INSERT INTO logs.log (timestamp, status, message, obj_num, task_name)
    VALUES (CURRENT_TIMESTAMP, 'ИНФО', 'Завершение инициализации витрины dm.dm_account_balance_f', NULL, 'dm.dm_account_balance_f');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO logs.log (timestamp, status, message, obj_num, task_name)
        VALUES (CURRENT_TIMESTAMP, 'ОШИБКА', 'Ошибка при инициализации витрины dm.dm_account_balance_f за ' || SQLERRM, NULL, 'dm.dm_account_balance_f');
        RAISE;
END;
$$;

-- процедура заполнения остатков по лицевым счетам
CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f (i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
	v_start_time timestamp := CURRENT_TIMESTAMP;
BEGIN
	INSERT INTO logs.log (timestamp, status, message, obj_num, task_name)
    VALUES (CURRENT_TIMESTAMP, 'ИНФО', 'Начало расчета витрины dm.dm_account_balance_f', NULL, 'ds.fill_account_balance_f');
    
    DELETE FROM dm.dm_account_balance_f WHERE on_date = i_OnDate;
    
    INSERT INTO dm.dm_account_balance_f (
    	on_date,
    	account_rk,
    	balance_out,
    	balance_out_rub
    )
    
    SELECT 
    	i_OnDate AS on_date,
    	acc.account_rk,
    	CASE acc.char_type
            WHEN 'А' THEN COALESCE(prev.balance_out, 0) + COALESCE(t.debet_amount, 0) - COALESCE(t.credit_amount, 0)
            WHEN 'П' THEN COALESCE(prev.balance_out, 0) - COALESCE(t.debet_amount, 0) + COALESCE(t.credit_amount, 0)
        END AS balance_out,
        CASE acc.char_type
            WHEN 'А' THEN COALESCE(prev.balance_out_rub, 0) + COALESCE(t.debet_amount_rub, 0) - COALESCE(t.credit_amount_rub, 0)
            WHEN 'П' THEN COALESCE(prev.balance_out_rub, 0) - COALESCE(t.debet_amount_rub, 0) + COALESCE(t.credit_amount_rub, 0)
        END AS balance_out_rub
    FROM DS.MD_ACCOUNT_D acc
    LEFT JOIN DM.DM_ACCOUNT_TURNOVER_F t
        ON acc.account_rk = t.account_rk
        AND t.on_date = i_OnDate
    LEFT JOIN DM.DM_ACCOUNT_BALANCE_F prev
        ON acc.account_rk = prev.account_rk
        AND prev.on_date = i_OnDate - 1
    LEFT JOIN DS.MD_EXCHANGE_RATE_D er
        ON acc.currency_rk = er.currency_rk
        AND i_OnDate BETWEEN er.data_actual_date AND COALESCE(er.data_actual_end_date, '9999-12-31'::DATE)
    WHERE i_OnDate BETWEEN acc.data_actual_date AND COALESCE(acc.data_actual_end_date, '9999-12-31'::DATE);
    
	INSERT INTO logs.log (timestamp, status, message, obj_num, task_name)
    VALUES (CURRENT_TIMESTAMP, 'ИНФО', 'Окончание расчета витрины dm.dm_account_balance_f за ' || 
            TO_CHAR(i_OnDate, 'YYYY-MM-DD') || '. Время выполнения: ' || 
            (CURRENT_TIMESTAMP - v_start_time)::text, NULL, 'ds.fill_account_balance_f');

EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO logs.log (timestamp, status, message, obj_num, task_name)
        VALUES (CURRENT_TIMESTAMP, 'ОШИБКА', 'Ошибка при расчете витрины dm.dm_account_balance_f за ' || 
                TO_CHAR(i_OnDate, 'YYYY-MM-DD') || ': ' || SQLERRM, NULL, 'ds.fill_account_balance_f');
        RAISE;
END;
$$;

-- цикл заполнения таблицы dm.dm_account_balance_f данными за январь
DO $$
BEGIN
    FOR i IN 1..31 LOOP
        CALL ds.fill_account_balance_f(TO_DATE('2018-01-' || i, 'YYYY-MM-DD'));
    END LOOP;
END $$;