import org.junit.Test;

import java.util.regex.Pattern;

public class StatementSplitTest {
    @Test
    public void paserSql(){
        String input = "--31\n" +
                "ALTER TABLE ea_stg.stg_revenue_third_prd_payment_third_bank_da\n" +
                "ADD IF NOT EXISTS PARTITION (date='${date}')\n" +
                "LOCATION 'hdfs://haruna/home/byte_ea_revenue/data/third/summary_bank/${date-1}'\n" +
                ";\n" +
                "--30\n" +
                "ALTER TABLE ea_stg.stg_revenue_third_prd_payment_third_bank_da\n" +
                "ADD IF NOT EXISTS PARTITION (date='${date-1}')\n" +
                "LOCATION 'hdfs://haruna/home/byte_ea_revenue/data/third/summary_bank/${date-2}'\n" +
                ";\n" +
                "--29\n" +
                "ALTER TABLE ea_stg.stg_revenue_third_prd_payment_third_bank_da\n" +
                "ADD IF NOT EXISTS PARTITION (date='${date-2}')\n" +
                "LOCATION 'hdfs://haruna/home/byte_ea_revenue/data/third/summary_bank/${date-3}'\n" +
                ";\n" +
                "--28\n" +
                "ALTER TABLE ea_stg.stg_revenue_third_prd_payment_third_bank_da\n" +
                "ADD IF NOT EXISTS PARTITION (date='${date-3}')\n" +
                "LOCATION 'hdfs://haruna/home/byte_ea_revenue/data/third/summary_bank/${date-4}'\n" +
                ";\n" +
                "--27\n" +
                "ALTER TABLE ea_stg.stg_revenue_third_prd_payment_third_bank_da\n" +
                "ADD IF NOT EXISTS PARTITION (date='${date-4}')\n" +
                "LOCATION 'hdfs://haruna/home/byte_ea_revenue/data/third/summary_bank/${date-5}'\n" +
                ";\n" +
                "--26\n" +
                "ALTER TABLE ea_stg.stg_revenue_third_prd_payment_third_bank_da\n" +
                "ADD IF NOT EXISTS PARTITION (date='${date-5}')\n" +
                "LOCATION 'hdfs://haruna/home/byte_ea_revenue/data/third/summary_bank/${date-6}'\n" +
                ";\n" +
                "--25\n" +
                "ALTER TABLE ea_stg.stg_revenue_third_prd_payment_third_bank_da\n" +
                "ADD IF NOT EXISTS PARTITION (date='${date-6}')\n" +
                "LOCATION 'hdfs://haruna/home/byte_ea_revenue/data/third/summary_bank/${date-7}'\n" +
                ";\n" +
                "--24\n" +
                "ALTER TABLE ea_stg.stg_revenue_third_prd_payment_third_bank_da\n" +
                "ADD IF NOT EXISTS PARTITION (date='${date-7}')\n" +
                "LOCATION 'hdfs://haruna/home/byte_ea_revenue/data/third/summary_bank/${date-8}'\n" +
                ";\n" +
                "--23\n" +
                "ALTER TABLE ea_stg.stg_revenue_third_prd_payment_third_bank_da\n" +
                "ADD IF NOT EXISTS PARTITION (date='${date-8}')\n" +
                "LOCATION 'hdfs://haruna/home/byte_ea_revenue/data/third/summary_bank/${date-9}'\n" +
                ";\n" +
                "--22\n" +
                "ALTER TABLE ea_stg.stg_revenue_third_prd_payment_third_bank_da\n" +
                "ADD IF NOT EXISTS PARTITION (date='${date-9}')\n" +
                "LOCATION 'hdfs://haruna/home/byte_ea_revenue/data/third/summary_bank/${date-10}'\n" +
                ";\n" +
                "--21\n" +
                "ALTER TABLE ea_stg.stg_revenue_third_prd_payment_third_bank_da\n" +
                "ADD IF NOT EXISTS PARTITION (date='${date-10}')\n" +
                "LOCATION 'hdfs://haruna/home/byte_ea_revenue/data/third/summary_bank/${date-11}'\n" +
                ";\n" +
                "\n" +
                "\n" +
                "\n" +
                "\n" +
                "WITH dm_accountant_time AS\n" +
                "(\n" +
                "    SELECT accountant_time AS atm\n" +
                "    ,regexp_replace(accountant_time,'-','') AS atmd\n" +
                "    ,lastdate_accountant_time AS lastdate_atm\n" +
                "    FROM ea_dm.dm_accountant_time\n" +
                "),\n" +
                "\n" +
                "--??????????????? transaction_serial_number +??????????????? account_serial_number +???????????? account_transaction\n" +
                "uniq_key_construct AS\n" +
                "(\n" +
                "    -- ???????????????\n" +
                "    -- https://bytedance.feishu.cn/docs/doccn6WZIOHGeitjMCbN7fAYOVe\n" +
                "    -- ???<<??????????????????>>??????\n" +
                "    SELECT \n" +
                "        CONCAT(bank_db_urid) AS  testing,\n" +
                "        COUNT(1)             AS  ct1\n" +
                "    FROM \n" +
                "        ea_stg.stg_revenue_third_prd_payment_third_bank_da\n" +
                "    JOIN \n" +
                "        dm_accountant_time \n" +
                "    ON \n" +
                "        1=1\n" +
                "    -- WHERE \n" +
                "    --     SUBSTRING(date,1,6)>=atmd\n" +
                "    GROUP BY \n" +
                "        CONCAT(bank_db_urid)\n" +
                ")\n" +
                "\n" +
                "\n" +
                "INSERT OVERWRITE TABLE ea_ods.ods_revenue_third_prd_payment_third_bank_ha PARTITION (`date` = '${date}',hour='23')\n" +
                "\n" +
                "SELECT\n" +
                "\n" +
                "    bank_db_urid                                       AS  bank_db_urid                   \t -- ????????????\n" +
                "    ,exchange_time                                     AS  exchange_time                  \t -- ????????????\n" +
                "    ,payment_water_code                                AS  payment_water_code             \t -- ???????????????\n" +
                "    ,account_type_name                                 AS  account_type_name              \t -- ??????????????????\n" +
                "    ,account_type_code                                 AS  account_type_code              \t -- ????????????????????????\n" +
                "    ,nostro_bank_account_number                        AS  nostro_bank_account_number     \t -- ????????????\n" +
                "    ,financial_system_name                             AS  financial_system_name          \t -- ????????????/??????????????????\n" +
                "    ,financial_system_code                             AS  financial_system_code          \t -- ??????????????????\n" +
                "    ,sub_account                                       AS  sub_account                    \t -- ?????????\n" +
                "    ,subhead_code                                      AS  subhead_code                   \t -- ??????\n" +
                "    ,merchant_number                                   AS  merchant_number                \t -- ?????????\n" +
                "    ,reciprocal_bank_account_number                    AS  reciprocal_bank_account_number \t -- ????????????\n" +
                "    ,reciprocal_bank_account_name                      AS  reciprocal_bank_account_name   \t -- ????????????\n" +
                "    ,exchange_currency_code                            AS  exchange_currency_code         \t -- ?????????\n" +
                "    ,exchange_money_amount                             AS  exchange_money_amount          \t -- ?????????????????????\n" +
                "    ,balance_money                                     AS  balance_money                  \t -- ??????\n" +
                "    ,account_transaction                               AS  account_transaction            \t -- ????????????\n" +
                "    ,product_name                                      AS  product_name                   \t -- ????????????\n" +
                "    ,degest_comment                                    AS  degest_comment                 \t -- ??????\n" +
                "    ,payments_direction                                AS  payments_direction             \t -- ????????????\n" +
                "    ,split(split(degest_comment, '\\073')[1], '\\\\+')[1] AS  merchant_order_number          \t -- ???????????????   \\073???????????? HIVE SQL????????????????????????????????????\n" +
                "    ,product_sum_name                                  AS  product_sum_name               \t -- ??????????????????\n" +
                "    ,accountant_time                                   AS  accountant_time                \t -- ??????????????????\n" +
                "    ,DATE_SUB(TO_DATE(date, 'yyyyMMdd'), 1)            AS  bi_file_date                      -- BI?????????????????????\n" +
                "\n" +
                "FROM\n" +
                "    ea_stg.stg_revenue_third_prd_payment_third_bank_da a\n" +
                "JOIN \n" +
                "    uniq_key_construct b\n" +
                "ON \n" +
                "    CONCAT(a.bank_db_urid) = b.testing\n" +
                "JOIN \n" +
                "    dm_accountant_time \n" +
                "ON \n" +
                "    1=1\n" +
                "-- -- WHERE \n" +
                "-- --     SUBSTRING(a.date,1,6)>=atmd\n" +
                "WHERE \n" +
                "    b.ct1 = 1\n" +
                ";\n" +
                "\n" +
                "\n" +
                "WITH dm_accountant_time AS\n" +
                "(\n" +
                "    SELECT accountant_time AS atm\n" +
                "    ,regexp_replace(accountant_time,'-','') AS atmd\n" +
                "    ,lastdate_accountant_time AS lastdate_atm\n" +
                "    FROM ea_dm.dm_accountant_time\n" +
                "),\n" +
                "uniq_key_construct AS\n" +
                "(\n" +
                "    -- https://bytedance.feishu.cn/docs/doccn6WZIOHGeitjMCbN7fAYOVe\n" +
                "    -- ???<<??????????????????>>??????\n" +
                "    SELECT \n" +
                "        CONCAT(bank_db_urid) AS  testing,\n" +
                "        COUNT(1)             AS  ct1\n" +
                "    FROM \n" +
                "        ea_stg.stg_revenue_third_prd_payment_third_bank_da\n" +
                "    JOIN \n" +
                "        dm_accountant_time \n" +
                "    ON \n" +
                "        1=1\n" +
                "    WHERE \n" +
                "        SUBSTRING(date,1,6)>=atmd\n" +
                "    GROUP BY \n" +
                "        CONCAT(bank_db_urid)\n" +
                ")\n" +
                "INSERT INTO TABLE ea_rpt.rpt_fin_revenue_third_error_list_ha PARTITION (`date` = '${date}',hour='${hour}')\n" +
                "SELECT\n" +
                "    '?????????' AS case_type\n" +
                "    ,b.testing AS case_key\n" +
                "    ,merchant_number AS merchant_number\n" +
                "    ,'????????????'\n" +
                "FROM \n" +
                "    ea_stg.stg_revenue_third_prd_payment_third_bank_da a\n" +
                "JOIN \n" +
                "    uniq_key_construct b\n" +
                "ON \n" +
                "    concat(a.bank_db_urid) = b.testing\n" +
                "JOIN \n" +
                "    dm_accountant_time ON 1=1 WHERE \n" +
                "    substring(a.date,1,6)>=atmd\n" +
                "AND \n" +
                "    b.ct1 > 1\n" +
                ";";
//        String[] split = input.split("\\s*;\\s*(?=([^']*'[^']*')*[^']*$)");
//        Arrays.stream(split).forEach(l -> System.out.println(l+"\n============="));
        String pattern = "\\s*;\\s*(?=([^\']*\'[^\']*\')*[^\']*$)";
        Pattern compile = Pattern.compile(pattern);
        String[] split = compile.split(input);
        for(String str : split){
            System.out.println(str);
            System.out.println();
        }

    }
}


























