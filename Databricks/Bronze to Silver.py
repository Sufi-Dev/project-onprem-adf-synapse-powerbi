# Databricks notebook source
# Bronze to Silver processing

from pyspark.sql import functions as F

bronze_path = "abfss://bronze@sanstrg.dfs.core.windows.net"
silver_path = "abfss://silver@sanstrg.dfs.core.windows.net"


# Schemas to process

schemas = [
    "SalesLT",
    "dbo"
]


# Primary keys used for deduplication

primary_keys = {
    "Customer": ["CustomerID"],
    "Product": ["ProductID"],
    "ProductCategory": ["ProductCategoryID"],
    "ProductModel": ["ProductModelID"],
    "Address": ["AddressID"],
    "CustomerAddress": ["CustomerID", "AddressID"],
    "SalesOrderHeader": ["SalesOrderID"],
    "SalesOrderDetail": ["SalesOrderID", "SalesOrderDetailID"]
}


# Process each schema

for schema_name in schemas:

    schema_path = f"{bronze_path}/{schema_name}/"

    table_names = [
        item.name.rstrip("/")
        for item in dbutils.fs.ls(schema_path)
        if item.isDir()
    ]

    print(f"Found {len(table_names)} tables in {schema_name}")


    for table_name in table_names:

        source_path = (
            f"{bronze_path}/{schema_name}/"
            f"{table_name}/{schema_name}.{table_name}.parquet"
        )

        target_path = (
            f"{silver_path}/{schema_name}/{table_name}/"
        )

        print(f"Processing {schema_name}.{table_name}")

        df = spark.read.parquet(source_path)

        source_count = df.count()



        # Trim whitespace from string columns

        string_columns = [
            field.name
            for field in df.schema.fields
            if field.dataType.simpleString() == "string"
        ]

        for column_name in string_columns:

            df = df.withColumn(
                column_name,
                F.trim(F.col(column_name))
            )



        # Standardize date and timestamp columns

        for field in df.schema.fields:

            column_name = field.name
            data_type = field.dataType.simpleString()

            if "date" in column_name.lower():

                if data_type == "string":

                    df = df.withColumn(
                        column_name,
                        F.to_timestamp(F.col(column_name))
                    )



        # Remove duplicate records

        if table_name in primary_keys:

            keys = [
                key
                for key in primary_keys[table_name]
                if key in df.columns
            ]

            if keys:
                df = df.dropDuplicates(keys)

        else:
            df = df.dropDuplicates()


        # Customer quality checks

        if table_name == "Customer":

            if "CustomerID" in df.columns:

                df = df.filter(
                    F.col("CustomerID").isNotNull()
                )

            if "FirstName" in df.columns:

                df = df.filter(
                    F.col("FirstName").isNotNull()
                )

            if "LastName" in df.columns:

                df = df.filter(
                    F.col("LastName").isNotNull()
                )



        # Product quality checks

        elif table_name == "Product":

            if "ProductID" in df.columns:

                df = df.filter(
                    F.col("ProductID").isNotNull()
                )

            if "Name" in df.columns:

                df = df.filter(
                    F.col("Name").isNotNull()
                )

            if "ListPrice" in df.columns:

                df = df.filter(
                    F.col("ListPrice").isNull()
                    | (F.col("ListPrice") >= 0)
                )

            if "StandardCost" in df.columns:

                df = df.filter(
                    F.col("StandardCost").isNull()
                    | (F.col("StandardCost") >= 0)
                )



        # Address quality checks

        elif table_name == "Address":

            if "AddressID" in df.columns:

                df = df.filter(
                    F.col("AddressID").isNotNull()
                )

            if "City" in df.columns:

                df = df.filter(
                    F.col("City").isNotNull()
                )

            if "PostalCode" in df.columns:

                df = df.withColumn(
                    "PostalCode",
                    F.trim(F.col("PostalCode"))
                )



        # Sales Order Header quality checks

        elif table_name == "SalesOrderHeader":

            if "SalesOrderID" in df.columns:

                df = df.filter(
                    F.col("SalesOrderID").isNotNull()
                )

            if "OrderDate" in df.columns:

                df = df.filter(
                    F.col("OrderDate").isNotNull()
                )

            if "TotalDue" in df.columns:

                df = df.filter(
                    F.col("TotalDue") >= 0
                )

            if "SubTotal" in df.columns:

                df = df.filter(
                    F.col("SubTotal") >= 0
                )

            if "TaxAmt" in df.columns:

                df = df.filter(
                    F.col("TaxAmt") >= 0
                )

            if "Freight" in df.columns:

                df = df.filter(
                    F.col("Freight") >= 0
                )



        # Sales Order Detail quality checks

        elif table_name == "SalesOrderDetail":

            if "SalesOrderID" in df.columns:

                df = df.filter(
                    F.col("SalesOrderID").isNotNull()
                )

            if "SalesOrderDetailID" in df.columns:

                df = df.filter(
                    F.col("SalesOrderDetailID").isNotNull()
                )

            if "ProductID" in df.columns:

                df = df.filter(
                    F.col("ProductID").isNotNull()
                )

            if "OrderQty" in df.columns:

                df = df.filter(
                    F.col("OrderQty") > 0
                )

            if "UnitPrice" in df.columns:

                df = df.filter(
                    F.col("UnitPrice") >= 0
                )

            if "UnitPriceDiscount" in df.columns:

                df = df.filter(
                    F.col("UnitPriceDiscount") >= 0
                )


        # Product Category quality checks

        elif table_name == "ProductCategory":

            if "ProductCategoryID" in df.columns:

                df = df.filter(
                    F.col("ProductCategoryID").isNotNull()
                )

            if "Name" in df.columns:

                df = df.filter(
                    F.col("Name").isNotNull()
                )


        # Product Model quality checks

        elif table_name == "ProductModel":

            if "ProductModelID" in df.columns:

                df = df.filter(
                    F.col("ProductModelID").isNotNull()
                )

            if "Name" in df.columns:

                df = df.filter(
                    F.col("Name").isNotNull()
                )


        # Add audit and lineage columns

        df = (
            df
            .withColumn(
                "_processed_at",
                F.current_timestamp()
            )
            .withColumn(
                "_source_schema",
                F.lit(schema_name)
            )
            .withColumn(
                "_source_table",
                F.lit(table_name)
            )
            .withColumn(
                "_source_layer",
                F.lit("bronze")
            )
        )


        silver_count = df.count()

        removed_count = source_count - silver_count


        # Write cleaned data to Silver

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(target_path)
        )


        print(
            f"{schema_name}.{table_name} completed | "
            f"Source rows: {source_count} | "
            f"Silver rows: {silver_count} | "
            f"Removed rows: {removed_count}"
        )
print("Bronze to Silver processing completed successfully")

