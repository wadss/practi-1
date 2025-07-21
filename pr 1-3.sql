-- создание таблицы DM.DM_F101_ROUND_F

CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f (
from_date date NOT NULL,
to_date date NOT NULL,
chapter char(1),
ledger_account char(5) NOT NULL,
characteristic char(1),
balance_in_rub numeric(23, 8),
balance_in_val numeric(23, 8),
balance_in_total numeric(23, 8),
turn_deb_rub numeric(23, 8),
turn_deb_val numeric(23, 8),
turn_deb_total numeric(23, 8),
turn_cre_rub numeric(23, 8),
turn_cre_val numeric(23, 8),
turn_cre_total numeric(23, 8),
balance_out_rub numeric(23, 8),
balance_out_val numeric(23, 8),
balance_out_total numeric(23, 8)
);

-- процедура формирования 101 формы
CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f (i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
	v_start_time timestamp := CURRENT_TIMESTAMP;
BEGIN
	INSERT INTO logs.log (timestamp, status, message, obj_num, task_name)
    VALUES (CURRENT_TIMESTAMP, 'ИНФО', 'Начало расчета витрины dm.dm_f101_round_f', NULL, 'ds.fill_f101_round_f');
    
    DELETE FROM dm.dm_f101_round_f WHERE from_date >= i_OnDate - interval '1 month' and to_date <= i_OnDate - interval '1 day';

	INSERT INTO dm.dm_f101_round_f (
		from_date,
		to_date,
		chapter,
		ledger_account,
		characteristic,
		balance_in_rub,
		balance_in_val,
		balance_in_total,
		turn_deb_rub,
		turn_deb_val,
		turn_deb_total,
		turn_cre_rub,
		turn_cre_val,
		turn_cre_total,
		balance_out_rub,
		balance_out_val,
		balance_out_total
	)
	SELECT
		(i_OnDate - interval '1 month')::date AS from_date,
		(i_OnDate - interval '1 day')::date AS to_date,
		mlas.chapter,
		LEFT(mad.account_number, 5) as ledger_account,
		mad.char_type as characteristic,
		SUM(dabf.balance_out_rub) FILTER (WHERE dabf.on_date = i_OnDate - INTERVAL '1 month 1 day' and mad.currency_code in ('810', '643')) AS balance_in_rub,
		SUM(dabf.balance_out_rub) FILTER (WHERE dabf.on_date = i_OnDate - INTERVAL '1 month 1 day' and mad.currency_code not in ('810', '643')) AS balance_in_val,
		SUM(dabf.balance_out_rub) FILTER (WHERE dabf.on_date = i_OnDate - INTERVAL '1 month 1 day') AS balance_in_total,
		SUM(datf.debet_amount_rub) FILTER (WHERE datf.on_date BETWEEN (i_OnDate - interval '1 month') and (i_OnDate - interval '1 day') and mad.currency_code in ('810', '643')) as turn_deb_rub,
		SUM(datf.debet_amount_rub) FILTER (WHERE datf.on_date BETWEEN (i_OnDate - interval '1 month') and (i_OnDate - interval '1 day') and mad.currency_code not in ('810', '643')) as turn_deb_val,
		SUM(datf.debet_amount_rub) FILTER (WHERE datf.on_date BETWEEN (i_OnDate - interval '1 month') and (i_OnDate - interval '1 day')) as turn_deb_total,
		SUM(datf.credit_amount_rub) FILTER (WHERE datf.on_date BETWEEN (i_OnDate - interval '1 month') and (i_OnDate - interval '1 day') and mad.currency_code in ('810', '643')) as turn_cre_rub,
		SUM(datf.credit_amount_rub) FILTER (WHERE datf.on_date BETWEEN (i_OnDate - interval '1 month') and (i_OnDate - interval '1 day') and mad.currency_code not in ('810', '643')) as turn_cre_val,
		SUM(datf.credit_amount_rub) FILTER (WHERE datf.on_date BETWEEN (i_OnDate - interval '1 month') and (i_OnDate - interval '1 day') and mad.currency_code in ('810', '643')) as turn_cre_total,
		SUM(dabf.balance_out_rub) FILTER (WHERE dabf.on_date = i_OnDate - INTERVAL '1 day' and mad.currency_code in ('810', '643')) AS balance_out_rub,
		SUM(dabf.balance_out_rub) FILTER (WHERE dabf.on_date = i_OnDate - INTERVAL '1 day' and mad.currency_code not in ('810', '643')) AS balance_out_val,
		SUM(dabf.balance_out_rub) FILTER (WHERE dabf.on_date = i_OnDate - INTERVAL '1 day') AS balance_out_total
	FROM ds.md_account_d AS mad
	LEFT JOIN ds.md_ledger_account_s AS mlas ON CAST(LEFT(mad.account_number, 5) AS INTEGER) = mlas.ledger_account
	LEFT JOIN dm.dm_account_balance_f AS dabf ON mad.account_rk = dabf.account_rk
	LEFT JOIN dm.dm_account_turnover_f AS datf ON mad.account_rk = datf.account_rk
	GROUP BY LEFT(mad.account_number, 5), mlas.chapter, mad.char_type;
		

    INSERT INTO logs.log (timestamp, status, message, obj_num, task_name)
    VALUES (CURRENT_TIMESTAMP, 'ИНФО', 'Окончание расчета витрины dm.dm_f101_round_f за ' || 
            TO_CHAR(i_OnDate, 'YYYY-MM-DD') || '. Время выполнения: ' || 
            (CURRENT_TIMESTAMP - v_start_time)::text, NULL, 'ds.fill_f101_round_f');

EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO logs.log (timestamp, status, message, obj_num, task_name)
        VALUES (CURRENT_TIMESTAMP, 'ОШИБКА', 'Ошибка при расчете витрины dm.dm_f101_round_f за ' || 
                TO_CHAR(i_OnDate, 'YYYY-MM-DD') || ': ' || SQLERRM, NULL, 'ds.fill_f101_round_f');
        RAISE;
END;
$$;


-- заполнение витрины dm.dm_f101_round_f
CALL dm.fill_f101_round_f('2018-02-01')
	
	
SELECT *
FROM ds.md_account_d AS mad
LEFT JOIN ds.md_ledger_account_s AS mlas ON CAST(LEFT(mad.account_number, 5) AS INTEGER) = mlas.ledger_account
LEFT JOIN dm.dm_account_balance_f AS dabf ON mad.account_rk = dabf.account_rk
LEFT JOIN dm.dm_account_turnover_f AS datf ON mad.account_rk = datf.account_rk
WHERE mad.currency_code not in ('810', '643') AND LEFT(mad.account_number, 5) = '30425'