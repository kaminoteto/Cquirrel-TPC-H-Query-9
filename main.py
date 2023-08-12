from pyflink.table import EnvironmentSettings, TableEnvironment
import mysql.connector
from typing import Tuple
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ProcessFunction


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


part_data_stream = table_env.from_path('part')
line_item_data_stream = table_env.from_path('lineitem')
partsupp_data_stream = table_env.from_path('partsupp')
orders_data_stream = table_env.from_path('orders')
supplier_data_stream = table_env.from_path('supplier')
nation_data_stream = table_env.from_path('nation')


# --------------Use operator to implement tpc-h query 9--------------
result_connect = part_data_stream \
    .connect(line_item_data_stream) \
    .key_by(lambda p: p.P_PARTKEY, lambda l: l.L_PARTKEY) \
    .process(ConnectPartAndLine()) \
    .connect(partsupp_data_stream) \
    .key_by(lambda t1: (t1.f2, t1.f3), lambda ps: (ps.PS_SUPPKEY, ps.PS_PARTKEY))\
    .process(ConnectTmpAndPartsupp()) \
    .connect(orders_data_stream) \
    .key_by(lambda t1: t1.f1, lambda o:o.O_ORDERKEY)\
    .process(ConnectTmpAndOrders()) \
    .connect(supplier_data_stream) \
    .key_by(lambda t1: t1.f3, lambda s:s.S_SUPPKEY)\
    .process(ConnectTmpAndSupplier()) \
    .connect(nation_data_stream) \
    .key_by(lambda t1: t1.f4, lambda n: n.N_NATIONKEY) \
    .process(ConnectTmpAndNation()) \

# Apply the necessary filters and computations
filtered_table = result_connect.filter(result_connect["P_NAME"].like("%green%"))
computed_table = filtered_table.select(
    filtered_table["N_NAME"].alias("NATION"),
    expr.year(filtered_table["O_ORDERDATE"]).alias("O_YEAR"),
    (filtered_table["L_EXTENDEDPRICE"] * (1 - filtered_table["L_DISCOUNT"])
     - filtered_table["PS_SUPPLYCOST"] * filtered_table["L_QUANTITY"]).alias("AMOUNT")
)
# Group by NATION and O_YEAR, and calculate SUM_PROFIT
grouped_table = computed_table.group_by(computed_table["NATION"], computed_table["O_YEAR"]) \
    .select(computed_table["NATION"], computed_table["O_YEAR"],
            computed_table["AMOUNT"].sum().alias("SUM_PROFIT"))

# Print the result and write to output.txt file
result = grouped_table
result.print()
result.write_text('output/output.txt', write_mode="OVERWRITE")
# Execute the job
env_settings.execute()



