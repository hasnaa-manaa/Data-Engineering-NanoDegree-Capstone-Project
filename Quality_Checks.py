import pandas as pd
from pyspark.sql import functions as F

def check_uniqueness(spark_df,table_name,PK ):
    """"
        function used verify unique key column has no duplicates
    
        arguments:-
            spark_df : spark dataframe to be verified
            table_name: to be shown in log
            PK : unique key 

    """
    if spark_df.select(PK).distinct().count() == spark_df.count():
        print (f"uniqeness check passed for table {table_name} unique column {PK}")
    else:
        print(f"uniqeness check failed for table {table_name} unique column {PK}")


def check_table_record_count(spark_df,table_name):
    """"
        function used to count rows of given spark dataframe
    
        arguments:-
            spark_df : spark dataframe to be counted
            table_name: to be shown in log

    """
    rows = spark_df.count()
    if (rows == 0):
        print(f"Data quality check failed for {spark_df} with zero records!")
    else:
        print(f"Data quality check passed for {table_name} with {rows} records")   
        
    return 0
    

def check_source_Destination_count(spark_table,source_df,table_name,PK):
    """"
        function used to verify continuity between source and destination
    
        arguments:-
            spark_table : final spark dataframe to be counted
            source_df : source spark dataframe to be compared against
            pk: column to be considered for count comparison
            table_name: to be shown in log

    """
    final_rows = spark_table.count()
    source_rows = source_df.select(PK).distinct().count() 
    if (final_rows == source_rows):
        print(f"Completness check passed for final table {table_name} compared to sataging")
    else:
        print(f"Completness check failed for final table {table_name} compared to sataging")   
        
    return 0
       
if __name__ == "__main__":
    main()