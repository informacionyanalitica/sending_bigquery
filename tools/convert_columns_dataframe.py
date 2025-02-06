import pandas as pd

class convertColumnDataFrame():
    def __init__(self,date_format: str = "%Y-%m-%d"):
        self.date_format = date_format

    def convert_columns_date(self,df,list_columns):
        try:
            for col in list_columns:
                df[col] = pd.to_datetime(df[col], format=self.date_format, errors="coerce")
            return df
        except Exception as error:
            print(error)

    def convert_columns_integer(self,df,list_columns):
        try:
            for col in list_columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
            return df
        except Exception as error:
            print(error)

    def convert_columns_float(self,df,list_columns):
        try:
            for col in list_columns:
               df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(float)
            return df
        except Exception as error:
            print(error)

    def convert_columns_string(self,df,list_columns):
        try:
            for col in list_columns:
                df[col] = df[col].astype(str)
            return df
        except Exception as error:
            print(error)