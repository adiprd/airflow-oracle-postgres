from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine, text
import logging
import cx_Oracle
import os
import io

# -----------------------------
# Default arguments
# -----------------------------
default_args = {
    'owner': 'adip radi triya',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# -----------------------------
# Initialize Oracle Client
# -----------------------------
def init_oracle_client():
    """Initialize Oracle Client with proper error handling"""
    try:
        instantclient_path = "/home/production/oracle/instantclient_23_9"

        if not os.path.exists(instantclient_path):
            raise FileNotFoundError(f"Oracle Instant Client not found at {instantclient_path}")

        required_files = ['libclntsh.so', 'libocci.so']
        for file in required_files:
            file_path = os.path.join(instantclient_path, file)
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Required Oracle file {file} not found in {instantclient_path}")

        os.environ["LD_LIBRARY_PATH"] = instantclient_path
        os.environ["ORACLE_HOME"] = instantclient_path

        cx_Oracle.init_oracle_client(lib_dir=instantclient_path)
        logging.info(f"Oracle client successfully initialized at {instantclient_path}")

    except Exception as e:
        logging.error(f"Oracle client initialization failed: {str(e)}", exc_info=True)
        raise

# -----------------------------
# ETL Function
# -----------------------------
def extract_transform_load():
    """
    Extract data from Oracle, transform, and load to PostgreSQL.
    Keeps PostgreSQL records older than 12 months.
    """
    oracle_connection = None
    pg_engine = None
    pg_conn = None
    pg_cursor = None

    try:
        init_oracle_client()

        # -----------------------------
        # Oracle Connection
        # -----------------------------
        oracle_conn = BaseHook.get_connection('oracle_conn_id')
        oracle_username = oracle_conn.login
        oracle_password = oracle_conn.password
        oracle_host = oracle_conn.host
        oracle_port = oracle_conn.port
        oracle_service = oracle_conn.schema

        dsn = cx_Oracle.makedsn(oracle_host, oracle_port, service_name=oracle_service)
        oracle_conn_str = f"{oracle_username}/{oracle_password}@{dsn}"

        logging.info(f"🔗 Connecting to Oracle ({oracle_host}:{oracle_port}/{oracle_service})...")
        oracle_connection = cx_Oracle.connect(oracle_conn_str)

        cursor = oracle_connection.cursor()
        cursor.execute("SELECT 1 FROM DUAL")
        test_result = cursor.fetchone()
        cursor.close()
        logging.info(f"Connected to Oracle successfully. Test query result: {test_result[0]}")

        # -----------------------------
        # PostgreSQL Connection
        # -----------------------------
        postgres_conn = BaseHook.get_connection('postgres_conn_id')
        postgres_username = postgres_conn.login
        postgres_password = postgres_conn.password
        postgres_host = postgres_conn.host
        postgres_port = postgres_conn.port
        postgres_db = postgres_conn.schema

        # Create SQLAlchemy engine for DELETE operation
        postgres_conn_str = (
            f"postgresql+psycopg2://{postgres_username}:{postgres_password}"
            f"@{postgres_host}:{postgres_port}/{postgres_db}"
        )

        logging.info(f"🔗 Connecting to PostgreSQL ({postgres_host}:{postgres_port}/{postgres_db})...")
        pg_engine = create_engine(postgres_conn_str, pool_pre_ping=True)

        with pg_engine.connect() as test_conn:
            test_result = test_conn.execute(text("SELECT 1")).scalar()
            logging.info(f"Connected to PostgreSQL successfully. Test query result: {test_result}")

        # -----------------------------
        # Oracle Query
        # -----------------------------
        query = """
        with data_item as 
        (
        select
            item_code,
            case 
                when item_code in ('AAA','AAB','AAC','AAD','AAE','AAF','AAG','AAH','AAJ','AAK','AAL','AAM',
                                'AAN','BAB','BAC','ACA','ACB','ACC','ADA','ADB','ADD','AFA','AFB','AFC',
                                'BFA','AGC','AGF','E3A','E3B','E4A','E4B','EAA','EAB','EAR','EAX','EBB',
                                'EBE','EBG','EBJ','EBR','ECA','ECB','EDF','EDH','EDP','EEE','EER','EGA',
                                'EGB','EHA','IBD','IBE','RL','ACD','ACE','EAS','EDG') then 'SAP'
                when item_code in ('IAA','GAA') then 'SHDE'
                when item_code = 'IAB' then 'SVDE'
                when item_code = 'IAO' then 'MBI'
                when item_code = 'IAP' then 'BND'
                when item_code IN ('IAU', 'IBB', 'IBF')  then 'BRG'
                when item_code = 'IAT' then 'ROLL'             
                when item_code = 'IAS' then 'GASKET'
                when item_code in ('IAG','AEA') then 'TR4'
                when item_code = 'IAK' then 'SGE'
            end sp_type
        from
            (select 
                trim(regexp_substr('AAA,AAB,AAC,AAD,AAE,AAF,AAG,AAH,AAJ,AAK,AAL,AAM,AAN,BAB,BAC,ACA,ACB,ACC,ADA,ADB,ADD,AFA,AFB,AFC,BFA,AGC,AGF,E3A,E3B,E4A,E4B,EAA,EAB,EAR,EAX,EBB,EBE,EBG,EBJ,EBR,ECA,ECB,EDF,EDH,EDP,EEE,EER,EGA,EGB,EHA,IAG,AEA,IBD,IBE,RL,IAA,IAK,IAB,IAO,IAP,IAU,IBB,IAT,ACD,ACE,EAS,EDG,GAA,IBF,IAS','[^,]+', 1, level) ) item_code 
            from dual 
            connect by regexp_substr('AAA,AAB,AAC,AAD,AAE,AAF,AAG,AAH,AAJ,AAK,AAL,AAM,AAN,BAB,BAC,ACA,ACB,ACC,ADA,ADB,ADD,AFA,AFB,AFC,BFA,AGC,AGF,E3A,E3B,E4A,E4B,EAA,EAB,EAR,EAX,EBB,EBE,EBG,EBJ,EBR,ECA,ECB,EDF,EDH,EDP,EEE,EER,EGA,EGB,EHA,IAG,AEA,IBD,IBE,RL,IAA,IAK,IAB,IAO,IAP,IAU,IBB,IAT,ACD,ACE,EAS,EDG,GAA,IBF,IAS', '[^,]+', 1, level) is not null
            order by level)
        )
        select  
                                        main_tab.cabang_dari_invoice "Cabang",
                                        ORGANIZATION_ID_INVOICE "Organization ID",
                                        main_tab.trx_date "Tgl Invoice",
                                        extract(year from main_tab.trx_date) "Tahun Invoice",
                                        extract(month from main_tab.trx_date) "Bulan Invoice",
                                        extract(day from main_tab.trx_date) "Tanggal Hari Invoice",
                                        main_tab.trx_number "No. Invoice",
                                        main_tab.ct_reference "No. SO",
                                        main_tab.ordered_date "Tgl SO",
                                        main_tab.trx_source "Tipe Invoice",
                                        main_tab.order_type "Order Type",
                                        main_tab.segment1 "Kode Barang",
                                        main_tab.description "Nama Barang",
                                        main_tab.price "Total Harga",
                                        case 
                                            when main_tab.TAX_CLASSIFICATION_CODE like '%10%' then main_tab.price * 110/100
                                            when main_tab.TAX_CLASSIFICATION_CODE like '%11%' then main_tab.price * 111/100
                                            else main_tab.price * 110/100
                                        end "Total Harga PPN",
                                        main_tab.pricelist,
                                        main_tab.invoice_retur,
                                        main_tab.no_do,
                                        main_tab.qty "Qty", main_tab.qty_retur,
                                        main_tab.price_dpp "Harga DPP/unit",
                                        case 
                                            when main_tab.TAX_CLASSIFICATION_CODE like '%10%' then main_tab.price_dpp * 110/100
                                            when main_tab.TAX_CLASSIFICATION_CODE like '%11%' then main_tab.price_dpp * 111/100
                                            else main_tab.price_dpp * 110/100
                                        end "Harga PPN/unit",
                                        main_tab.werehouse "Warehouse",
                                        main_tab.nama_cust "Nama Customer",
                                        main_tab.ship_to_kota "Kota Ship To",
                                        di.sp_type "Tipe Sparepart",
                                        case 
                                            when upper(main_tab.order_type) like '%TOKOQUICK%' then 'TOKOQUICK'
                                            when upper(main_tab.pricelist) like '%QT%' then 'QUICK TRUCK'
                                            else 'UMUM'
                                        end "Tipe Penjualan"
                                    from
                                    (select
                                        trx_date,
                                        trx_number,
                                        ct_reference,
                                        ordered_date,
                                        name trx_source,
                                        order_type,
                                        ot_return,
                                        segment1,
                                        description,
                                        price,
                                        pricelist,
                                        case 
                                            when order_type like '%LN' then 'HO'
                                            when order_type like '%DN' then REGEXP_SUBSTR(order_type, '[^-]+')
                                        end kode_cabang,
                                        case 
                                            when order_type like '%LN' then 'Ekspor'
                                            when order_type like '%DN' 
                                                then (select distinct 
                                                        trim(replace(substr(haou.name, 4), '(OU)', ''))
                                                    from
                                                        hr_all_organization_units haou
                                                    where 
                                                        haou.organization_id = org_id)
                                        end cabang,
                                        invoice_retur,
                                        no_do,
                                        qty, qty_retur,
                                        price_dpp,
                                        organization_code werehouse,
                                        sold_to_customer nama_cust,
                                        ship_to_city ship_to_kota,
                                        TAX_CLASSIFICATION_CODE,
                                        cabang_dari_invoice,
                                        ORGANIZATION_ID_INVOICE
                                    from
                                    (SELECT DISTINCT ooha.attribute1 nama, ooha.attribute2 alamat1,
                                                    ooha.attribute3 alamat2, ooha.attribute4 kota,
                                                    ooha.attribute5 no_telp, rctla.line_number,
                                                    rcta.trx_date , rcta.trx_number , rcta.ct_reference ,
                                                    ooha.ordered_date , rctta.NAME ,
                                                    ottt.NAME order_type, msib.segment1 ,
                                                    rctla.description ,
                                                    NVL (rctla.quantity_invoiced, rctla.quantity_credited) qty,
                                                    rctla.unit_selling_price
                                                    * NVL (rcta.exchange_rate, 1) price_dpp,
                                                    NVL (rctla.quantity_invoiced,
                                                        rctla.quantity_credited
                                                        )
                                                    * (rctla.unit_selling_price * NVL (rcta.exchange_rate, 1)) price,
                                                    (SELECT   SUM (  NVL (rctla1.quantity_invoiced,
                                                                        rctla1.quantity_credited
                                                                        )
                                                                * (  rctla1.unit_selling_price
                                                                    * NVL (rcta1.exchange_rate, 1)
                                                                    )
                                                                )
                                                        FROM ra_customer_trx_lines_all rctla1,
                                                            ra_customer_trx_all rcta1
                                                        WHERE rcta1.customer_trx_id = rctla1.customer_trx_id
                                                        AND rcta1.customer_trx_id = rcta.customer_trx_id
                                                    GROUP BY rcta.trx_number) dpp,
                                                    NVL ((SELECT   SUM (NVL (aaa.line_adjusted, 0))
                                                            FROM ar_adjustments_all aaa,
                                                                ar_receivables_trx_all arta
                                                            WHERE rcta.customer_trx_id = aaa.customer_trx_id
                                                            AND aaa.receivables_trx_id =
                                                                                        arta.receivables_trx_id
                                                            AND arta.NAME != 'Apply Uang Muka Customer'
                                                        GROUP BY rcta.trx_number),
                                                        0
                                                        ) Adjusting,
                                                    mp.organization_code ,
                                                    rac_sold_party.party_name sold_to_customer,
                                                    rac_sold.account_number,
                                                    rac_bill_party.party_name bill_to_customer,
                                                    raa_bill_loc.city,
                                                    rac_ship_party.party_name ship_to_customer,
                                                    CASE
                                                    WHEN rac_sold_party.party_name LIKE
                                                                                        '%Eceran'
                                                        THEN ooha.attribute4
                                                    ELSE raa_ship_loc.city
                                                    END ship_to_city,
                                                    ooha.shipping_instructions,
                                                    (SELECT DISTINCT MIN (w.batch_id)
                                                                FROM oe_order_headers_all h,
                                                                    oe_order_lines_all l,
                                                                    wsh_delivery_details w
                                                            WHERE h.header_id = l.header_id
                                                                AND l.line_id = w.source_line_id
                                                                AND h.order_number = ooha.order_number) no_do,
                                                    (SELECT qlht.NAME pricelist
                                                    FROM qp_list_headers_tl qlht
                                                    WHERE qlht.list_header_id = ooha.price_list_id) pricelist,
                                                    rcta.org_id,
                                                    case 
                                                        when ottt.name like '%LN' then 'Ekspor'
                                                        else
                                                        (select
                                                            trim(replace(substr(haou1.name, 4), '(OU)', ''))
                                                        from
                                                            hr_all_organization_units haou1
                                                        where
                                                            haou1.organization_id = rcta.org_id)
                                                    end cabang,
                                                    (SELECT DISTINCT ottl.NAME pricelist_first
                                                    FROM ra_customer_trx_all rcta_retur,
                                                            ra_customer_trx_all rcta_first,
                                                            oe_order_headers_all ooha_first,
                                                            oe_transaction_types_tl ottl
                                                    WHERE ooha_first.order_number = rcta_first.ct_reference
                                                        AND rcta_first.customer_trx_id = rcta_retur.previous_customer_trx_id
                                                        AND ottl.transaction_type_id = ooha_first.order_type_id
                                                        AND rcta_retur.trx_number = RCTA.TRX_NUMBER) ot_return,
                                                    -- tambahan
                                                    (select string_agg(rcta_retur.trx_number)
                                                    from ra_customer_trx_lines_all rctl_retur,
                                                            ra_customer_trx_all rcta_retur
                                                    where 1=1
                                                        and rctla.customer_trx_line_id = rctl_retur.previous_customer_trx_line_id
                                                        AND rcta_retur.customer_trx_id = rctl_retur.customer_trx_id
                                                        and rctl_retur.LINE_TYPE = 'LINE') invoice_retur,
                                                    nvl((select sum(rctl_retur.quantity_credited)
                                                    from ra_customer_trx_lines_all rctl_retur
                                                    where 1=1
                                                        and rctla.customer_trx_line_id = rctl_retur.previous_customer_trx_line_id
                                                        and rctl_retur.LINE_TYPE = 'LINE'),0) qty_retur,
                                                    rctla.TAX_CLASSIFICATION_CODE,
                                                    hou.NAME cabang_dari_gudang,
                                                    houinv.NAME cabang_dari_invoice,
                                                    houinv.ORGANIZATION_ID ORGANIZATION_ID_INVOICE
                                            FROM ra_customer_trx_lines_all rctla,
                                                    ra_customer_trx_all rcta,
                                                    ra_cust_trx_types_all rctta,
                                                    mtl_system_items_b msib,
                                                    oe_order_headers_all ooha,
                                                    oe_transaction_types_tl ottt,
                                                    oe_order_lines_all oola,
                                                    mtl_parameters mp,
                                                    hz_parties rac_sold_party,
                                                    hz_parties rac_bill_party,
                                                    hz_locations raa_bill_loc,
                                                    hz_parties rac_ship_party,
                                                    hz_locations raa_ship_loc,
                                                    hz_cust_accounts_all rac_sold,
                                                    hz_cust_accounts_all rac_bill,
                                                    hz_party_sites raa_bill_ps,
                                                    fnd_territories_vl ft_bill,
                                                    hz_cust_accounts_all rac_ship,
                                                    hz_party_sites raa_ship_ps,
                                                    fnd_territories_vl ft_ship,
                                                    hz_cust_acct_sites_all raa_ship,
                                                    hz_cust_acct_sites_all raa_bill,
                                                    hz_cust_site_uses_all su_ship,
                                                    hz_cust_site_uses_all su_bill,
                                                    fnd_user fu,
                                                    org_organization_definitions ood,
                                                    HR_ORGANIZATION_UNITS hou,
                                                    HR_ORGANIZATION_UNITS houinv
                                            WHERE rcta.customer_trx_id = rctla.customer_trx_id
                                                AND rcta.cust_trx_type_id = rctta.cust_trx_type_id
                                                AND rctla.inventory_item_id = msib.inventory_item_id(+)
                                                AND rcta.ct_reference = ooha.order_number(+)
                                                AND ooha.order_type_id = ottt.transaction_type_id(+)
                                                AND ooha.header_id = oola.header_id
                                                AND rctla.warehouse_id = mp.organization_id(+)
                                                AND msib.organization_id(+) = 81
                                                AND rac_sold.party_id = rac_sold_party.party_id(+)
                                                AND rac_bill.party_id = rac_bill_party.party_id
                                                AND raa_bill_loc.location_id = raa_bill_ps.location_id
                                                AND raa_bill_loc.country = ft_bill.territory_code(+)
                                                AND rac_ship.party_id = rac_ship_party.party_id(+)
                                                AND raa_ship_loc.location_id(+) = raa_ship_ps.location_id
                                                AND raa_ship_loc.country = ft_ship.territory_code(+)
                                                AND rcta.sold_to_customer_id = rac_sold.cust_account_id(+)
                                                AND rcta.bill_to_customer_id = rac_bill.cust_account_id
                                                AND raa_bill.party_site_id = raa_bill_ps.party_site_id
                                                AND rcta.ship_to_customer_id = rac_ship.cust_account_id(+)
                                                AND raa_ship.party_site_id = raa_ship_ps.party_site_id(+)
                                                AND su_ship.cust_acct_site_id = raa_ship.cust_acct_site_id(+)
                                                AND raa_ship.party_site_id = raa_ship_ps.party_site_id(+)
                                                AND su_bill.cust_acct_site_id = raa_bill.cust_acct_site_id
                                                AND raa_bill.party_site_id = raa_bill_ps.party_site_id
                                                AND rcta.ship_to_site_use_id = su_ship.site_use_id(+)
                                                AND rcta.bill_to_site_use_id = su_bill.site_use_id
                                                AND mp.ORGANIZATION_ID = ood.ORGANIZATION_ID
                                                AND ood.OPERATING_UNIT = hou.ORGANIZATION_ID
                                                AND rcta.ORG_ID = houinv.ORGANIZATION_ID
                                                AND rctta.NAME IN ('Inv. Penj. Lokal', 'Inv. Penj. Export', 'CM Inv. Penj. Lokal', 'CM Inv. Penj. Export')
                                                AND rac_sold_party.party_name NOT IN
                                                    ('KHS PAMERAN', 'KHS DEMO', 'KHS CUSTOMER KLAIM',
                                                        'KHS RISET & TESTING')
                                                AND (   oola.source_type_code = 'INTERNAL'
                                                    OR (    oola.source_type_code = 'EXTERNAL'
                                                        AND UPPER (ottt.NAME) NOT LIKE '%TRAKTOR%'
                                                        )
                                                    OR oola.source_type_code IS NULL
                                                    )
                                                AND rctla.line_type = 'LINE'
                                                AND ooha.created_by = fu.user_id
                                                -- tambahan kondisi omset sparepart
                                                -- AND rac_sold.account_number not in (9638, 13867, 14307, 14127)
        --                                        AND case 
        --                                                when rcta.org_id IN (82, 143) 
        --                                                     and rac_sold.account_number in (9638, 13867, 14307, 14127)
        --                                                     then 0 
        --                                                when rcta.org_id IN (82) 
        --                                                     and rac_sold.account_number in (14748, 27502) -- exclude KHS CUSTOMER E-COMERCE LAZADA SHOPEE
        --                                                     then 0 
        --                                                     else 1 end = 1
                                                AND (ottt.name like '%DN' 
                                                    or ottt.name like '%LN'
                                                    or ottt.name like '%TOKOQUICK%')
                                                AND (ottt.name like '%SAP%' 
                                                    or ottt.name like '%SHDE%'
                                                    or ottt.name like '%SGE%'
                                                    or ottt.name like '%SVDE%'
                                                    or ottt.name like '%Mitsuboshi%'
                                                    or ottt.name like '%Bando%'
                                                    or ottt.name like '%SKF%'
                                                    or ottt.name like '%Nachi%'
                                                    or ottt.name like '%Roll%'
                                                    or ottt.name like '%Bearing%Quick%All%'
                                                    or ottt.name like '%Retur%'
                                                    or ottt.name like '%Gasket%'
                                                    or ottt.name like '%TOKOQUICK%')
                                                AND msib.segment1 not like 'JAD%' 
                                                AND msib.segment1 not like 'QA2%'
                                        ---parameter
                                                AND trunc(rcta.trx_date) BETWEEN trunc(ADD_MONTHS(SYSDATE,-12)) AND trunc(SYSDATE)
        --                                        AND rcta.trx_number BETWEEN NVL (:no_inv, rcta.trx_number)
        --                                                                AND NVL (:no_inv_2, rcta.trx_number)
        --                                        AND NVL (rcta.ct_reference, 1) BETWEEN NVL
        --                                                                                  (:no_so,
        --                                                                                   NVL (rcta.ct_reference,
        --                                                                                        1
        --                                                                                       )
        --                                                                                  )
        --                                                                           AND NVL
        --                                                                                  (:no_so_2,
        --                                                                                   NVL (rcta.ct_reference,
        --                                                                                        1
        --                                                                                       )
        --                                                                                  )
        --                                        AND rac_sold_party.party_id =
        --                                                                 NVL (:nama_cust, rac_sold_party.party_id)
        --                                        AND NVL (ottt.NAME, 1) = NVL (:order_type, NVL (ottt.NAME, 1))
        --                                        AND NVL(mp.organization_code, '1') = COALESCE (:p_org_code, mp.organization_code, '1')
                                                -- pembatasan 
        --                                        &P_WHERE
                                                        )dd) main_tab,
                                        data_item di
                                    where di.item_code = substr(segment1,0 ,3)
                                    AND case 
                                                when upper(main_tab.order_type) like '%RETUR%'
                                                and (upper(main_tab.pricelist) like '%SP%' 
                                                    or upper(main_tab.pricelist) like '%RH%ROLL%' 
                                                    or upper(main_tab.pricelist) like '%SPARE%PART%'
                                                    or upper(main_tab.pricelist) like '%TOKOQUICK%' ) then 1
                                                when upper(main_tab.order_type) not like '%RETUR%' then 1
                                                else 0
                                                end = 1
        --                              AND case 
        --                                        when main_tab.order_type like '%Retur%'
        --                                         and (main_tab.ot_return like '%SAP%' 
        --                                             or main_tab.ot_return like '%SHDE%'
        --                                             or main_tab.ot_return like '%SGE%'
        --                                             or main_tab.ot_return like '%SVDE%'
        --                                             or main_tab.ot_return like '%Mitsuboshi%'
        --                                             or main_tab.ot_return like '%Bando%'
        --                                             or main_tab.ot_return like '%SKF%'
        --                                             or main_tab.ot_return like '%Nachi%'
        --                                             or main_tab.ot_return like '%Roll%'
        --                                             or main_tab.ot_return like '%Bearing%Quick%All%'
        --                                             or main_tab.ot_return like '%Gasket%') then 1
        --                                        when main_tab.order_type not like '%Retur%' then 1
        --                                        else 0
        --                                       end = 1
        ORDER BY        werehouse, order_type
        """

        # -----------------------------
        # Extract data from Oracle
        # -----------------------------
        logging.info("Extracting data from Oracle...")
        df = pd.read_sql(query, oracle_connection)
        logging.info(f"Extracted {len(df)} rows from Oracle")

        # -----------------------------
        # Transform data - FIX COLUMN NAMES
        # -----------------------------
        logging.info("Transforming data...")
        column_mapping = {
            'Cabang': 'cabang',
            'Organization ID' : 'organization_id',
            'Tahun Invoice' : 'tahun_invoice',
	    	'Bulan Invoice' : 'bulan_invoice',
	    	'Tanggal Hari Invoice' : 'tanggal_hari_invoice',
            'Tgl Invoice': 'tgl_invoice',
            'No. Invoice': 'no_invoice',
            'No. SO': 'no_so',
            'Tgl SO': 'tgl_so',
            'Tipe Invoice': 'tipe_invoice',
            'Order Type': 'order_type',
            'Kode Barang': 'kode_barang',
            'Nama Barang': 'nama_barang',
            'Total Harga': 'total_harga',
            'Total Harga PPN': 'total_harga_ppn',
            'pricelist': 'pricelist',
            'invoice_retur': 'invoice_retur',
            'no_do': 'no_do',
            'Qty': 'qty',
            'qty_retur': 'qty_retur',
            'Harga DPP/unit': 'harga_dpp_per_unit',
            'Harga PPN/unit': 'harga_ppn_per_unit',
            'Warehouse': 'warehouse',
            'Nama Customer': 'nama_customer',
            'Kota Ship To': 'kota_ship_to',
            'Tipe Sparepart': 'tipe_sparepart',
            'Tipe Penjualan': 'tipe_penjualan'
        }
        df = df.rename(columns=column_mapping)

        # FIXED: Ensure all column names are lowercase to match PostgreSQL table
        df.columns = [col.lower() for col in df.columns]

        # Check if we have the expected columns
        expected_columns = ['cabang','organization_id' , 'tahun_invoice' , 'bulan_invoice' , 'tanggal_hari_invoice',
    		         	   'tgl_invoice' , 'no_invoice', 'no_so', 'tgl_so', 'tipe_invoice',
                           'order_type', 'kode_barang', 'nama_barang', 'total_harga', 'total_harga_ppn',
                           'pricelist', 'invoice_retur', 'no_do', 'qty', 'qty_retur', 'harga_dpp_per_unit',
                           'harga_ppn_per_unit', 'warehouse', 'nama_customer', 'kota_ship_to',
                           'tipe_sparepart', 'tipe_penjualan']

        # Log column comparison
        logging.info(f"Expected columns: {expected_columns}")
        logging.info(f"Actual columns: {list(df.columns)}")

        # Check for missing columns
        missing_columns = set(expected_columns) - set(df.columns)
        if missing_columns:
            logging.warning(f"⚠️issing columns: {missing_columns}")

        # Check for extra columns
        extra_columns = set(df.columns) - set(expected_columns)
        if extra_columns:
            logging.warning(f"Extra columns: {extra_columns}")

        date_columns = ['tgl_invoice', 'tgl_so']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
                logging.info(f"Converted {col} to datetime")

        # Handle NaN values
        df = df.fillna('')

        logging.info(f"Data transformation completed. Final columns: {list(df.columns)}")

        # -----------------------------
        # Load to PostgreSQL - DELETE then INSERT using COPY
        # -----------------------------

        # Hitung cutoff date: hari ini - 12 bulan
        cutoff_date = datetime.now() - pd.DateOffset(months=12)
        logging.info(f"eleting data newer than {cutoff_date.strftime('%Y-%m-%d')}...")

        # Use engine.begin() for transaction context (auto-commit)
        with pg_engine.begin() as conn:
            result = conn.execute(
                text("DELETE FROM mb.khs_customer_transactions_sparepart WHERE tgl_invoice >= :cutoff_date and tahun_invoice <> 9999"),
                {"cutoff_date": cutoff_date}
            )
            deleted_rows = result.rowcount
            logging.info(f"Deleted {deleted_rows} rows from PostgreSQL")

        # FIXED: Use COPY method dengan koneksi manual
        logging.info("Inserting new data into PostgreSQL using COPY...")

        # Convert DataFrame to CSV in memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, header=False, sep='\t', na_rep='NULL')
        csv_buffer.seek(0)

        # Get raw connection manually (tanpa context manager)
        pg_conn = pg_engine.raw_connection()
        try:
            pg_cursor = pg_conn.cursor()

            # Get column names - use lowercase to match table
            columns = ', '.join([f'"{col}"' for col in df.columns])

            # Log the COPY command for debugging
            copy_sql = f"""
            COPY mb.khs_customer_transactions_sparepart ({columns})
            FROM STDIN WITH (FORMAT CSV, DELIMITER E'\t', NULL 'NULL')
            """
            logging.info(f"Executing COPY command with columns: {columns}")

            pg_cursor.copy_expert(copy_sql, csv_buffer)
            pg_conn.commit()
            logging.info(f"Successfully loaded {len(df)} records to PostgreSQL")

        except Exception as e:
            pg_conn.rollback()
            logging.error(f"COPY failed: {str(e)}")
            raise e
        finally:
            if pg_cursor:
                pg_cursor.close()
            if pg_conn:
                pg_conn.close()

        logging.info("ETL process completed successfully!")

    except Exception as e:
        logging.error(f"ETL process failed: {str(e)}", exc_info=True)
        raise

    finally:
        logging.info("Cleaning up connections...")
        if oracle_connection:
            try:
                oracle_connection.close()
                logging.info("Oracle connection closed")
            except Exception as e:
                logging.warning(f"rror closing Oracle connection: {str(e)}")

        if pg_cursor:
            try:
                pg_cursor.close()
                logging.info("PostgreSQL cursor closed")
            except Exception as e:
                logging.warning(f"rror closing PostgreSQL cursor: {str(e)}")

        if pg_conn:
            try:
                pg_conn.close()
                logging.info("PostgreSQL connection closed")
            except Exception as e:
                logging.warning(f"rror closing PostgreSQL connection: {str(e)}")

        if pg_engine:
            try:
                pg_engine.dispose()
                logging.info("PostgreSQL engine disposed")
            except Exception as e:
                logging.warning(f"rror disposing PostgreSQL engine: {str(e)}")

        logging.info("All connections cleaned up")

# -----------------------------
# DAG Definition
# -----------------------------
dag = DAG(
    'etl_khs_customer_transactions_sparepart_14',
    default_args=default_args,
    description='ETL from Oracle to PostgreSQL for Sparepart Transactions - Keep data >12 months',
    schedule_interval='0 0 * * *',
    catchup=False,
    tags=['etl', 'oracle', 'postgres', 'sparepart']
)

etl_task = PythonOperator(
    task_id='extract_transform_load_sparepart',
    python_callable=extract_transform_load,
    dag=dag,
)
