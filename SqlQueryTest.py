from pyflink.table import EnvironmentSettings, TableEnvironment
import mysql.connector
from JoinOperator import ConnectPartAndLine, ConnectTmpAndPartsupp, ConnectTmpAndOrders, ConnectTmpAndSupplier, ConnectTmpAndNation
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

connection = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="12345678",
        database='Cquirrel'
    )
# ----------------Create flink table from mysql database---------------------

customer_create = """
        CREATE TABLE customer (
            C_CUSTKEY INTEGER,
            C_NAME        VARCHAR(25) NOT NULL,
            C_ADDRESS     VARCHAR(40) NOT NULL,
            C_NATIONKEY   INTEGER NOT NULL,
            C_PHONE       CHAR(15) NOT NULL,
            C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
            C_MKTSEGMENT  CHAR(10) NOT NULL,
            C_COMMENT     VARCHAR(117) NOT NULL,
            PRIMARY KEY (C_CUSTKEY)  NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://localhost:3306/Cquirrel',
            'table-name' = 'customer',
            'username' = 'root',
            'password' = '12345678',
            'driver' = 'com.mysql.cj.jdbc.Driver'
        )
    """
lineitem_create = """
        CREATE TABLE lineitem ( 
            L_ORDERKEY INTEGER NOT NULL,
            L_PARTKEY     INTEGER NOT NULL,
            L_SUPPKEY     INTEGER NOT NULL,
            L_LINENUMBER  INTEGER NOT NULL,
            L_QUANTITY    DECIMAL(15,2) NOT NULL,
            L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
            L_DISCOUNT    DECIMAL(15,2) NOT NULL,
            L_TAX         DECIMAL(15,2) NOT NULL,
            L_RETURNFLAG  CHAR(1) NOT NULL,
            L_LINESTATUS  CHAR(1) NOT NULL,
            L_SHIPDATE    DATE NOT NULL,
            L_COMMITDATE  DATE NOT NULL,
            L_RECEIPTDATE DATE NOT NULL,
            L_SHIPINSTRUCT CHAR(25) NOT NULL,
            L_SHIPMODE     CHAR(10) NOT NULL,
            L_COMMENT      VARCHAR(44) NOT NULL,
    primary key(L_ORDERKEY,L_LINENUMBER) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://localhost:3306/Cquirrel',
            'table-name' = 'lineitem',
            'username' = 'root',
            'password' = '12345678',
            'driver' = 'com.mysql.cj.jdbc.Driver'
        )
    """
nation_create = """
        CREATE TABLE nation (
            N_NATIONKEY  INT,
            N_NAME       CHAR(25) NOT NULL,
            N_REGIONKEY  INTEGER NOT NULL,
            N_COMMENT    VARCHAR(152),
            PRIMARY KEY (N_NATIONKEY)  NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://localhost:3306/Cquirrel',
            'table-name' = 'nation',
            'username' = 'root',
            'password' = '12345678',
            'driver' = 'com.mysql.cj.jdbc.Driver'
        )
    """
orders_create = """
        CREATE TABLE orders (
            O_ORDERKEY INTEGER,
            O_CUSTKEY        INTEGER NOT NULL,
            O_ORDERSTATUS    CHAR(1) NOT NULL,
            O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
            O_ORDERDATE      DATE NOT NULL,
            O_ORDERPRIORITY  CHAR(15) NOT NULL,
            O_CLERK          CHAR(15) NOT NULL,
            O_SHIPPRIORITY   INTEGER NOT NULL,
            O_COMMENT        VARCHAR(79) NOT NULL,
            PRIMARY KEY (O_ORDERKEY)  NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://localhost:3306/Cquirrel',
            'table-name' = 'orders',
            'username' = 'root',
            'password' = '12345678',
            'driver' = 'com.mysql.cj.jdbc.Driver'
        )
    """
part_create = """
        CREATE TABLE part (
            P_PARTKEY INTEGER,
            P_NAME        VARCHAR(55) NOT NULL,
            P_MFGR        CHAR(25) NOT NULL,
            P_BRAND       CHAR(10) NOT NULL,
            P_TYPE        VARCHAR(25) NOT NULL,
            P_SIZE        INTEGER NOT NULL,
            P_CONTAINER   CHAR(10) NOT NULL,
            P_RETAILPRICE DECIMAL(15,2) NOT NULL,
            P_COMMENT     VARCHAR(23) NOT NULL,
            PRIMARY KEY (P_PARTKEY)  NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://localhost:3306/Cquirrel',
            'table-name' = 'part',
            'username' = 'root',
            'password' = '12345678',
            'driver' = 'com.mysql.cj.jdbc.Driver'
        )
    """

partsupp_create = """
        CREATE TABLE partsupp (
            PS_PARTKEY INTEGER NOT NULL,
            PS_SUPPKEY     INTEGER NOT NULL,
            PS_AVAILQTY    INTEGER NOT NULL,
            PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
            PS_COMMENT     VARCHAR(199) NOT NULL, 
            primary key (PS_PARTKEY, PS_SUPPKEY) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://localhost:3306/Cquirrel',
            'table-name' = 'partsupp',
            'username' = 'root',
            'password' = '12345678',
            'driver' = 'com.mysql.cj.jdbc.Driver'
        )
    """
region_create = """
        CREATE TABLE region (
            R_REGIONKEY INTEGER,
            R_NAME       CHAR(25) NOT NULL,
            R_COMMENT    VARCHAR(152), 
            primary key (R_REGIONKEY) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://localhost:3306/Cquirrel',
            'table-name' = 'region',
            'username' = 'root',
            'password' = '12345678',
            'driver' = 'com.mysql.cj.jdbc.Driver'
        )
    """
supplier_create = """
        CREATE TABLE supplier (
            S_SUPPKEY INTEGER,
            S_NAME        CHAR(25) NOT NULL,
            S_ADDRESS     VARCHAR(40) NOT NULL,
            S_NATIONKEY   INTEGER NOT NULL,
            S_PHONE       CHAR(15) NOT NULL,
            S_ACCTBAL     DECIMAL(15,2) NOT NULL,
            S_COMMENT     VARCHAR(101) NOT NULL, 
            primary key (S_SUPPKEY) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://localhost:3306/Cquirrel',
            'table-name' = 'supplier',
            'username' = 'root',
            'password' = '12345678',
            'driver' = 'com.mysql.cj.jdbc.Driver'
        )
    """
table_env.execute_sql(customer_create)
table_env.execute_sql(lineitem_create)
table_env.execute_sql(nation_create)
table_env.execute_sql(orders_create)
table_env.execute_sql(part_create)
table_env.execute_sql(partsupp_create)
table_env.execute_sql(region_create)
table_env.execute_sql(supplier_create)

# --------------Convert the tables to streams--------------
stream1 = table_env.from_path('nation')
# stream2 = t_env.to_append_stream(t_env.from_path('orders'))

# Apply keyBy(), process(), and connect() operators
# result_stream = stream1.key_by(lambda x: x['key']).process(ProcessFunction()).connect(stream2)
result_stream = stream1
# result_stream.get_schema()
# result_stream.execute().print()

#table_env.execute_sql('select * from nation').print()

tpch_q9 = """
SELECT
     NATION,
     O_YEAR,
     SUM(AMOUNT) AS SUM_PROFIT
FROM
     (
		SELECT
			N_NAME AS NATION,
			YEAR(O_ORDERDATE) AS O_YEAR,
			L_EXTENDEDPRICE * (1 - L_DISCOUNT) - PS_SUPPLYCOST * L_QUANTITY AS AMOUNT
		FROM
			part,
			lineitem,
			partsupp,
			orders,
			supplier,
			nation
		WHERE
			S_SUPPKEY = L_SUPPKEY
			AND PS_SUPPKEY = L_SUPPKEY
			AND PS_PARTKEY = L_PARTKEY
			AND P_PARTKEY = L_PARTKEY
			AND O_ORDERKEY = L_ORDERKEY
			AND S_NATIONKEY = N_NATIONKEY
			AND P_NAME LIKE '%green%'
     ) AS PROFIT
GROUP BY
     NATION,
     O_YEAR

"""
test_query ="""
SELECT
     P_PARTKEY,
     P_NAME
FROM
    part,
    lineitem
WHERE
P_PARTKEY = L_PARTKEY

AND P_NAME LIKE '%green%'

"""
table_env.execute_sql(tpch_q9).print()



# Sink the result to console
# print(result_stream.n_nationkey)

# Execute the job
# env_settings.execute("Flink Job")
