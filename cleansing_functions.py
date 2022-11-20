import pandas as pd

def clean_dataset(df,series_columns = [],columns_to_delete=[],columns_to_split = [],splits=2,splitter = ','):
    """"
        function used to drop duplicates, remove nulls, get first value of list columns drop unnecessary columns from dataframe
    
        arguments:-
            df : pandas dataframe to be cleansed
            series_columns : list of columns that are of type series to 
                              be converted into string by considering first value of series
            columns_to_delete : list of columns that should be deleted 
            columns_to_split : list of columns that should be splited into number n of columns 
            splits: number of columns that should be generated from each column
            splitter: character to split column with

    """
    df.dropna()
    print(f"null records were dropped from dataset")
    
    df.drop_duplicates()
    print(f"Duplicate records were dropped from dataset")
    
    if columns_to_delete != []:
        for column in series_columns:
            df[f'Main {column}'] = df[column].str.split(',', expand=False).str[0]
            print(f'list column {column} was converted to string by considering first value of list')

    if columns_to_split != []:
        df = df.drop(columns = columns_to_delete,axis=1)
        print(f'columns {columns_to_delete} were dropped')
        
    if columns_to_split !=[]:
        for column in columns_to_split:
            for i in range (1,splits+1):
                df[f'{column}{i}'] = df[column].str.split(',', expand=False).str[i-1].str.strip() 
            print(f'column {column} was split into {splits} columns')

    return df


def adjust_date_column(df,date_column,new_date):
    """
        function to adjust date string format and add datetime column to dataset

        argument:

        df: pandas ddataframe
        date_column :pandas dataframe column in string format

    """
    df[new_date] = df[date_column].str.split(' to', expand=False).str[0].str.strip() 
    
    #dealing with year only records
    
    df['adj_date'] = pd.to_numeric(df[new_date], errors="coerce")
    
    df['adj_date'] ='Jan 1, '+ df['adj_date'][df['adj_date'].notna()==True].astype(int).astype(str)
    
    df[new_date][df['adj_date'].notna()==True] = df['adj_date'] 
    
    df = df.drop(columns = [date_column,'adj_date'],axis=1)
    
    df[new_date]=df[new_date].str.replace(',','-')
    
    df[new_date]=pd.to_datetime(df[new_date], format='%b %d- %Y',errors='coerce')
    

    print(f'string column {date_column} was replaced with date column {new_date}')
    
    return df

if __name__ == "__main__":
    main()