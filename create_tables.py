import pandas as pd
from pyspark.sql import functions as F

def create_dim_anime(spark_df,output_path = 'tables/'):
    """"
        function used to create anime dimension table
    
        arguments:-
            spark_df : spark dataframe to be loaded
            output : path to which parquet is to be added

    """
    df = spark_df.select(["MAL_ID", "Name", "English_Name", "Japanese_Name", "Type", "Main_Studios", "Main_Producers", "Main_Licensors", "Source", 
                          "Rating","Genres1","Genres2","Genres3"]).dropDuplicates(["MAL_ID"])
                          
    df.write.mode("overwrite").parquet( output_path + "DimAnime")
    
    print(f"Table DimAnime was witten successfully to {output_path}DimAnime")


def create_dim_anime_viewer(spark_df,output_path = 'tables/'):
    """"
        function used to create anime_viewer dimension table
    
        arguments:-
            spark_df : spark dataframe to be loaded
            output : path to which parquet is to be added

    """
    df = spark_df.select(["User_ID","Watched_Episodes"]).dropDuplicates(["User_ID"])
                          
    df.write.mode("overwrite").parquet( output_path + "DimAnimeViewer")
    
    print(f"Table DimAnimeViewer was witten successfully to {output_path}DimAnimeViewer")
    

def create_dim_watching_status(spark_df,output_path = 'tables/'):
    """"
        function used to create watching_status dimension table
    
        arguments:-
            spark_df : spark dataframe to be loaded
            output : path to which parquet is to be added

    """
    df = spark_df.select(["Status_ID","Status_Description"]).dropDuplicates(["Status_ID"])
                          
    df.write.mode("overwrite").parquet( output_path + "DimWathcingStatus")
    
    print(f"Table DimWathcingStatus was witten successfully to {output_path}DimWathcingStatus")

def create_dim_date(spark_df,output_path = 'tables/'):
    """"
        function used to create date dimension table
    
        arguments:-
            spark_df : spark dataframe to be loaded
            output : path to which parquet is to be added

    """
    df = spark_df.select(["Date","Year","Month_Name","Month",
                          "Quarter","ID"]).dropDuplicates(["ID"])\
                          .withColumn('Formatted_Date', F.to_date("Date","dd/MM/yyyy"))
                          
    df.write.mode("overwrite").parquet( output_path + "DimDate")
    
    print(f"Table DimDate was witten successfully to {output_path}DimDate")
    
    
def create_fact_user_rating(spark_df,output_path = 'tables/'):
    """"
        function used to create fact user_rating table
    
        arguments:-
            spark_df : spark dataframe to be loaded
            output : path to which parquet is to be added

    """
    df = spark_df.select(["User_ID","Anime_ID","Rating","Watching_Status_ID"]).dropDuplicates(["User_ID","Anime_ID"])
                          
    df.write.mode("overwrite").parquet( output_path + "FactUserRating")
    
    print(f"Table FactUserRating was witten successfully to {output_path}FactUserRating")
    

def create_fact_anime_measures(spark_df,dim_date,output_path = 'tables/'):
    """"
        function used to create fact anime_measures
    
        arguments:-
            spark_df : spark dataframe to be loaded
            dim_date : spark dataframe holding date formatted and date id 
            output : path to which parquet is to be added

    """
    df= spark_df.select(["*"])\
                .join(dim_date, (spark_df.First_Show == dim_date.Formatted_Date), how='left')
    df = df.select(["MAL_ID","Score","ID","Duration",
                    "Ranked","Popularity","Members","Favorites",
                    "Watching","Completed","On-Hold","Dropped",
                    "Plan_to_Watch","Score-10","Score-1",
                    "Score-2","Score-3","Score-4","Score-5",
                    "Score-6","Score-7","Score-8","Score-9"])\
                    .withColumn("Score",F.when(df.Score == "Unknown",0).otherwise(df.Score).cast("float"))\
                    .withColumnRenamed("ID", "First_Show_Date_ID")\
                    .withColumn("Ranked",F.when(df.Ranked == "Unknown",0).otherwise(df.Ranked).cast("float"))\
                    .withColumn("Popularity",F.when(df.Popularity == "Unknown",0).otherwise(df.Popularity).cast("int"))\
                    .withColumn("Members",F.when(df.Members == "Unknown",0).otherwise(df.Members).cast("int"))\
                    .withColumn("Favorites",F.when(df.Favorites == "Unknown",0).otherwise(df.Favorites).cast("int"))\
                    .withColumn("Watching",F.when(df.Watching == "Unknown",0).otherwise(df.Watching).cast("int"))\
                    .withColumn("Completed",F.when(df.Completed == "Unknown",0).otherwise(df.Completed).cast("int"))\
                    .withColumn("On-Hold",F.when(df['On-Hold'] == "Unknown",0).otherwise(df['On-Hold']).cast("int"))\
                    .withColumn("Dropped",F.when(df.Dropped == "Unknown",0).otherwise(df.Dropped).cast("int"))\
                    .withColumn("Plan_to_Watch",F.when(df.Plan_to_Watch == "Unknown",0).otherwise(df.Plan_to_Watch).cast("int"))\
                    .withColumn("Score-10",F.when(df['Score-10'] == "Unknown",0).otherwise(df['Score-10']).cast("int"))\
                    .withColumn("Score-1",F.when(df['Score-1'] == "Unknown",0).otherwise(df['Score-1']).cast("int"))\
                    .withColumn("Score-2",F.when(df['Score-2'] == "Unknown",0).otherwise(df['Score-2']).cast("int"))\
                    .withColumn("Score-3",F.when(df['Score-3'] == "Unknown",0).otherwise(df['Score-3']).cast("int"))\
                    .withColumn("Score-4",F.when(df['Score-4'] == "Unknown",0).otherwise(df['Score-4']).cast("int"))\
                    .withColumn("Score-5",F.when(df['Score-5'] == "Unknown",0).otherwise(df['Score-5']).cast("int"))\
                    .withColumn("Score-6",F.when(df['Score-6'] == "Unknown",0).otherwise(df['Score-6']).cast("int"))\
                    .withColumn("Score-7",F.when(df['Score-7'] == "Unknown",0).otherwise(df['Score-7']).cast("int"))\
                    .withColumn("Score-8",F.when(df['Score-8'] == "Unknown",0).otherwise(df['Score-8']).cast("int"))\
                    .withColumn("Score-9",F.when(df['Score-9'] == "Unknown",0).otherwise(df['Score-9']).cast("int")).dropDuplicates(["MAL_ID"])

                          
    df.write.mode("overwrite").parquet( output_path + "FactAnimeMeasures")
    
    print(f"Table FactAnimeMeasures was witten successfully to {output_path}FactAnimeMeasures")
    
    
if __name__ == "__main__":
    main()