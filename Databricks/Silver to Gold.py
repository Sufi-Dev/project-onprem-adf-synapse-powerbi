# Databricks notebook source
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

        # Same behavior as original code
        new_name = new_name.lstrip("_")

        # Check for duplicate names
        if new_name in new_columns:
            raise ValueError(
                f"Duplicate column name found after renaming: '{new_name}'"
            )

        new_columns.append(new_name)

    # Rename all columns at once
    return df.toDF(*new_columns)
    

sales_tables =[]

for table in dbutils.fs.ls("abfss://silver@sanstrg.dfs.core.windows.net/SalesLT/"):
    sales_tables.append(table.name.rstrip("/"))

dbo_tables =[]

for table in dbutils.fs.ls("abfss://silver@sanstrg.dfs.core.windows.net/dbo/"):
    dbo_tables.append(table.name.rstrip("/"))


for table in sales_tables:
    table_path = f"abfss://silver@snstrg.dfs.core.windows.net/SalesLT/{table}/"
    table_path_dest = f"abfss://gold@sanstrg.dfs.core.windows.net/SalesLT/{table}/"
    df = spark.read.format("delta").load(table_path)
    df = rename_columns_to_snake_case(df)
    df.write.mode("overwrite").format("delta").save(table_path_dest)
    print(f"{table} has been Processed")
 

for table in dbo_tables:
    table_path = f"abfss://silver@sanstrg.dfs.core.windows.net/dbo/{table}/"
    table_path_dest = f"abfss://gold@sanstrg.dfs.core.windows.net/dbo/{table}/"
    df = spark.read.format("delta").load(table_path)
    df = rename_columns_to_snake_case(df)
    df.write.mode("overwrite").format("delta").save(table_path_dest)
    print(f"{table} has been Processed")

