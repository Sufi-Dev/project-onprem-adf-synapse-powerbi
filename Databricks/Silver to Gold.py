# Databricks notebook source

from pyspark.sql import functions as F


silver_path = "abfss://silver@sanstrg.dfs.core.windows.net"
gold_path = "abfss://gold@sanstrg.dfs.core.windows.net"

def rename_columns_to_snake_case(df):

    new_columns = []

    for column_name in df.columns:

        new_name = ""

        for index, char in enumerate(column_name):

            if (
                char.isupper()
                and index > 0
                and not column_name[index - 1].isupper()
            ):
                new_name += "_" + char.lower()

            else:
                new_name += char.lower()

        new_name = new_name.lstrip("_")

        if new_name in new_columns:
            raise ValueError(
                f"Duplicate column name after renaming: {new_name}"
            )

        new_columns.append(new_name)

    return df.toDF(*new_columns)


def read_silver_table(table_name):

    path = f"{silver_path}/SalesLT/{table_name}/"

    df = (
        spark.read
        .format("delta")
        .load(path)
    )

    return rename_columns_to_snake_case(df)



customer_df = read_silver_table("Customer")
address_df = read_silver_table("Address")
customer_address_df = read_silver_table("CustomerAddress")

product_df = read_silver_table("Product")
product_category_df = read_silver_table("ProductCategory")
product_model_df = read_silver_table("ProductModel")

sales_header_df = read_silver_table("SalesOrderHeader")
sales_detail_df = read_silver_table("SalesOrderDetail")



# Customer dimension

dim_customer = (
    customer_df.alias("c")
    .join(
        customer_address_df.alias("ca"),
        F.col("c.customer_id") == F.col("ca.customer_id"),
        "left"
    )
    .join(
        address_df.alias("a"),
        F.col("ca.address_id") == F.col("a.address_id"),
        "left"
    )
    .select(
        F.col("c.customer_id"),
        F.col("c.title"),
        F.col("c.first_name"),
        F.col("c.middle_name"),
        F.col("c.last_name"),
        F.col("c.company_name"),
        F.col("c.email_address"),
        F.col("c.phone"),
        F.col("a.address_line1").alias("address_line_1"),
        F.col("a.address_line2").alias("address_line_2"),
        F.col("a.city"),
        F.col("a.state_province"),
        F.col("a.country_region"),
        F.col("a.postal_code"),
        F.current_timestamp().alias("gold_processed_at")
    )
    .dropDuplicates(["customer_id"])
)



# Product dimension

dim_product = (
    product_df.alias("p")
    .join(
        product_category_df.alias("pc"),
        F.col("p.product_category_id")
        == F.col("pc.product_category_id"),
        "left"
    )
    .join(
        product_model_df.alias("pm"),
        F.col("p.product_model_id")
        == F.col("pm.product_model_id"),
        "left"
    )
    .select(
        F.col("p.product_id"),
        F.col("p.name").alias("product_name"),
        F.col("p.product_number"),
        F.col("p.color"),
        F.col("p.standard_cost"),
        F.col("p.list_price"),
        F.col("p.size"),
        F.col("p.weight"),
        F.col("pc.product_category_id"),
        F.col("pc.name").alias("product_category"),
        F.col("pm.product_model_id"),
        F.col("pm.name").alias("product_model"),
        F.current_timestamp().alias("gold_processed_at")
    )
)



# Sales fact table

fact_sales = (
    sales_detail_df.alias("d")
    .join(
        sales_header_df.alias("h"),
        F.col("d.sales_order_id")
        == F.col("h.sales_order_id"),
        "inner"
    )
    .select(
        F.col("d.sales_order_detail_id"),
        F.col("d.sales_order_id"),
        F.col("h.customer_id"),
        F.col("d.product_id"),
        F.col("h.order_date"),
        F.col("h.due_date"),
        F.col("h.ship_date"),
        F.col("h.status"),
        F.col("d.order_qty"),
        F.col("d.unit_price"),
        F.col("d.unit_price_discount"),
        F.col("h.sub_total"),
        F.col("h.tax_amt"),
        F.col("h.freight"),
        F.col("h.total_due")
    )
)


# Add useful sales measures

fact_sales = (
    fact_sales
    .withColumn(
        "gross_sales_amount",
        F.col("order_qty") * F.col("unit_price")
    )
    .withColumn(
        "discount_amount",
        (
            F.col("order_qty")
            * F.col("unit_price")
            * F.col("unit_price_discount")
        )
    )
    .withColumn(
        "net_sales_amount",
        (
            F.col("order_qty")
            * F.col("unit_price")
            * (F.lit(1) - F.col("unit_price_discount"))
        )
    )
    .withColumn(
        "order_year",
        F.year("order_date")
    )
    .withColumn(
        "order_month",
        F.month("order_date")
    )
    .withColumn(
        "order_quarter",
        F.quarter("order_date")
    )
    .withColumn(
        "order_date_key",
        F.date_format(
            F.col("order_date"),
            "yyyyMMdd"
        ).cast("int")
    )
    .withColumn(
        "gold_processed_at",
        F.current_timestamp()
    )
)


# Date dimension

date_range = (
    sales_header_df
    .select(
        F.min("order_date").alias("min_date"),
        F.max("order_date").alias("max_date")
    )
    .collect()[0]
)


min_date = date_range["min_date"]
max_date = date_range["max_date"]


dim_date = (
    spark.sql(
        f"""
        SELECT explode(
            sequence(
                to_date('{min_date}'),
                to_date('{max_date}'),
                interval 1 day
            )
        ) AS full_date
        """
    )
    .withColumn(
        "date_key",
        F.date_format("full_date", "yyyyMMdd").cast("int")
    )
    .withColumn(
        "year",
        F.year("full_date")
    )
    .withColumn(
        "quarter",
        F.quarter("full_date")
    )
    .withColumn(
        "month",
        F.month("full_date")
    )
    .withColumn(
        "month_name",
        F.date_format("full_date", "MMMM")
    )
    .withColumn(
        "week_of_year",
        F.weekofyear("full_date")
    )
    .withColumn(
        "day_of_month",
        F.dayofmonth("full_date")
    )
    .withColumn(
        "day_name",
        F.date_format("full_date", "EEEE")
    )
    .withColumn(
        "is_weekend",
        F.dayofweek("full_date").isin(1, 7)
    )
)



# Customer sales summary

customer_sales_summary = (
    fact_sales
    .groupBy("customer_id")
    .agg(
        F.countDistinct("sales_order_id").alias("total_orders"),
        F.sum("order_qty").alias("total_units_purchased"),
        F.round(
            F.sum("net_sales_amount"),
            2
        ).alias("total_sales"),
        F.round(
            F.avg("net_sales_amount"),
            2
        ).alias("average_line_value"),
        F.max("order_date").alias("last_order_date")
    )
)


customer_sales_summary = (
    customer_sales_summary.alias("s")
    .join(
        dim_customer.alias("c"),
        F.col("s.customer_id") == F.col("c.customer_id"),
        "left"
    )
    .select(
        F.col("s.customer_id"),
        F.col("c.first_name"),
        F.col("c.last_name"),
        F.col("c.company_name"),
        F.col("c.city"),
        F.col("c.country_region"),
        F.col("s.total_orders"),
        F.col("s.total_units_purchased"),
        F.col("s.total_sales"),
        F.col("s.average_line_value"),
        F.col("s.last_order_date")
    )
)



# Product sales summary

product_sales_summary = (
    fact_sales.alias("s")
    .join(
        dim_product.alias("p"),
        F.col("s.product_id") == F.col("p.product_id"),
        "left"
    )
    .groupBy(
        F.col("s.product_id"),
        F.col("p.product_name"),
        F.col("p.product_category")
    )
    .agg(
        F.sum("order_qty").alias("units_sold"),
        F.countDistinct("sales_order_id").alias("order_count"),
        F.round(
            F.sum("gross_sales_amount"),
            2
        ).alias("gross_sales"),
        F.round(
            F.sum("discount_amount"),
            2
        ).alias("discount_amount"),
        F.round(
            F.sum("net_sales_amount"),
            2
        ).alias("net_sales")
    )
)



# Monthly sales summary

monthly_sales_summary = (
    fact_sales
    .groupBy(
        "order_year",
        "order_month"
    )
    .agg(
        F.countDistinct("sales_order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum("order_qty").alias("units_sold"),
        F.round(
            F.sum("net_sales_amount"),
            2
        ).alias("net_sales")
    )
    .orderBy(
        "order_year",
        "order_month"
    )
)



gold_tables = {
    "dim_customer": dim_customer,
    "dim_product": dim_product,
    "dim_date": dim_date,
    "fact_sales": fact_sales,
    "customer_sales_summary": customer_sales_summary,
    "product_sales_summary": product_sales_summary,
    "monthly_sales_summary": monthly_sales_summary
}



for table_name, df in gold_tables.items():

    destination = (
        f"{gold_path}/SalesLT/{table_name}/"
    )

    row_count = df.count()

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(destination)
    )

    print(
        f"{table_name} completed | "
        f"Rows written: {row_count}"
    )

print("Silver to Gold processing completed successfully")
