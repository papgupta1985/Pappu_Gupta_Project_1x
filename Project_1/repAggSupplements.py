import pandas as pd
import loadDataSQlite as dbcon
import json

'''
Created by          : Pappu gupta
Create Date         : 25-04-2021
Description         : Generate Report - aggregate of the total number of supplements given per year 
Versions            : Created 
'''

pd.set_option('display.width', 500)
pd.set_option('display.max_columns', 10)


def aggSuppliments(database=r"D:\Database\NEH_GRANTS_DB.db"):
    try:
        # get database connection
        con = dbcon.create_connection(database)

        # query preparation
        query = "select Supplements  from NEH_GRANTS_2010_2019 where Supplements is not NULL"

        # load data into dataframe
        df = pd.read_sql_query(query, con)

        # Explode Supplements with date
        df = df.assign(Supplements=df['Supplements'].str.split(';')).explode('Supplements')

        # separate supplements & date
        df = df['Supplements'].str.split(expand=True).rename(columns=lambda x: f"Column_{x + 1}")

        # Remove '[]' from date
        df['Column_2'] = df['Column_2'].apply(lambda x: x.replace('[', '').replace(']', ''))

        # Extract Year
        df['year'] = pd.DatetimeIndex(df['Column_2']).year

        # Aggregation of total number of supplements per year
        df['No_of_Supplement'] = 0
        df = df.groupby(['year']).count()

        # remove columns from dataframe
        df.drop(['Column_1', 'Column_2'], axis=1, inplace=True)


        # Preparing output as per reporting requirement
        reportOutput = df.to_dict()

        # Print output
        #print(reportOutput)

        # Writing output to Json file
        vjson = json.dumps(reportOutput)
        f = open(r"D:\Reports\Agg_Supplements.json", "w")
        f.write(vjson)
        f.close()

        con.close()
    except Exception as e:
        print('Error in function countAggGrant - ' + str(e))

if __name__ == '__main__':
    aggSuppliments()
