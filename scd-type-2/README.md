# How to use Databricks / Delta Lake to ingest files to an SCD Type 2 table 

To run the example you need to:
1. Create table with the script in migrations/customer_address.sql\
2. Upload data to your Workspace home directory in Databricks
3. Substitute [user name] with your user name in Databricks in `merge_scd_type2.py`
4. Import and run the notebook in `merge_scd_type2.py`
