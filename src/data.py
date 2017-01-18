"""Responsible for facading the access to the data in the CSV files."""

from datetime import datetime
from decimal import Decimal
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType

from context import sql


def read(path, schema=None):
    """Read the file from the path using the supplied schema."""
    return sql.read.option("header", "false") \
                   .option("delimiter", "|") \
                   .csv(path, schema=schema)


def convert_to_timestamp(val):
    """Convert the string timestamp to a TimestampType column."""
    if len(val) == 17:
        return datetime.strptime(val, "%m/%d/%y %H:%M:%S")
    return datetime.strptime(val, "%m/%d/%Y %H:%M:%S")

timestamp_udf = UserDefinedFunction(lambda x: convert_to_timestamp(x), TimestampType())


def convert_amount_to_decimal(val):
    """Convert the string amount to a DecimalType column."""
    try:
        return Decimal(val.replace("$", ""))
    except:
        return None

amount_udf = UserDefinedFunction(lambda x: convert_amount_to_decimal(x), DecimalType())


def trim_whitespace(val):
    """Trim leading and trailing whitespace."""
    return val.strip()

whitespace_udf = UserDefinedFunction(lambda x: trim_whitespace(x), StringType())


def customer():
    """Get the customer dataset."""
    customer_schema = StructType([StructField("customer_id", IntegerType(), True),
                                  StructField("customer_first_name", StringType(), True),
                                  StructField("customer_last_name", StringType(), True),
                                  StructField("phone_number", StringType(), True)
                                  ])
    return read('./data/Customer.txt', customer_schema) \
        .registerTempTable("customer")


def customer_extended():
    """Get the customer extended dataset."""
    customer_extended_schema = StructType([StructField("id", IntegerType(), True),
                                           StructField("first_name", StringType(), True),
                                           StructField("last_name", StringType(), True),
                                           StructField("home_phone", StringType(), True),
                                           StructField("mobile_phone", StringType(), True),
                                           StructField("gender", StringType(), True),
                                           StructField("current_street_address", StringType(), True),
                                           StructField("current_city", StringType(), True),
                                           StructField("current_state", StringType(), True),
                                           StructField("current_country", StringType(), True),
                                           StructField("current_zip", StringType(), True),
                                           StructField("permanent_street_address", StringType(), True),
                                           StructField("permanent_city", StringType(), True),
                                           StructField("permanent_state", StringType(), True),
                                           StructField("permanent_country", StringType(), True),
                                           StructField("permanent_zip", StringType(), True),
                                           StructField("office_street", StringType(), True),
                                           StructField("office_city", StringType(), True),
                                           StructField("office_state", StringType(), True),
                                           StructField("office_country", StringType(), True),
                                           StructField("office_zip", StringType(), True),
                                           StructField("personal_email_address", StringType(), True),
                                           StructField("work_email_address", StringType(), True),
                                           StructField("twitter_id", StringType(), True),
                                           StructField("facebook_id", StringType(), True),
                                           StructField("linkedin_id", StringType(), True)
                                           ])
    return read('./data/Customer_Extended.txt', customer_extended_schema) \
        .registerTempTable("customer_extended")


def product():
    """Get the product dataset."""
    product_schema = StructType([StructField("product_id", IntegerType(), True),
                                 StructField("product_name", StringType(), True),
                                 StructField("product_type", StringType(), True),
                                 StructField("product_version", StringType(), True),
                                 StructField("product_price", StringType(), True)
                                 ])
    return read('./data/Product.txt', product_schema) \
        .withColumn('d_product_price', amount_udf("product_price")) \
        .withColumn('d_product_name', whitespace_udf("product_name")) \
        .registerTempTable("product")


def refund():
    """Get the refund dataset."""
    refund_schema = StructType([StructField("refund_id", IntegerType(), True),
                                StructField("original_transaction_id", IntegerType(), True),
                                StructField("customer_id", IntegerType(), True),
                                StructField("product_id", IntegerType(), True),
                                StructField("timestamp", StringType(), True),
                                StructField("refund_amount", StringType(), True),
                                StructField("refund_quantity", IntegerType(), True)
                                ])
    return read('./data/Refund.txt', refund_schema) \
        .withColumn('d_timestamp', timestamp_udf("timestamp")) \
        .withColumn('d_refund_amount', amount_udf("refund_amount")) \
        .registerTempTable("refund")


def sales():
    """Get the sales dataset."""
    sales_schema = StructType([StructField("transaction_id", IntegerType(), True),
                               StructField("customer_id", IntegerType(), True),
                               StructField("product_id", IntegerType(), True),
                               StructField("timestamp", StringType(), True),
                               StructField("total_amount", StringType(), True),
                               StructField("total_quantity", IntegerType(), True),
                               ])
    return read('./data/Sales.txt', sales_schema) \
        .withColumn('d_timestamp', timestamp_udf("timestamp")) \
        .withColumn('d_total_amount', amount_udf("total_amount")) \
        .registerTempTable("sales")
