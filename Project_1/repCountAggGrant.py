import pandas as pd
import loadDataSQlite as dbcon
import json

'''
Created by          : Pappu gupta
Create Date         : 25-04-2021
Description         : Generate Report - Count of each project per state with aggregated grants for each state
Versions            : Created 
'''

pd.set_option('display.width', 500)
pd.set_option('display.max_columns', 10)


def countAggGrant(database=r"D:\Database\NEH_GRANTS_DB.db"):
    try:

        # get database connection
        con = dbcon.create_connection(database)

        # query preparation
        query = "select InstState, ProjectTitle, ApprovedOutright, AwardOutright, OriginalAmount, SupplementAmount  from NEH_GRANTS_2010_2019"

        # load data into dataframe
        df = pd.read_sql_query(query, con)

        # Calculate aggregated grants for each state
        df_2 = df
        grouped_df_2 = df_2.groupby(['InstState'])
        df_2['Agg_Approved_Outright'] = grouped_df_2['ApprovedOutright'].transform(sum)
        df_2['Agg_Award_Outright'] = grouped_df_2['AwardOutright'].transform(sum)
        df_2['Agg_OriginalAmount'] = grouped_df_2['OriginalAmount'].transform(sum)
        df_2['Agg_Supplement_Amt'] = grouped_df_2['SupplementAmount'].transform(sum)

        # remove columns from dataframe
        df_2.drop(['ApprovedOutright','AwardOutright', 'OriginalAmount', 'SupplementAmount'], axis=1, inplace=True)

        # Calculate count of each project per state
        df_2['count'] =0
        df_2 = df_2.groupby(['InstState', 'ProjectTitle', 'Agg_Approved_Outright','Agg_Award_Outright','Agg_OriginalAmount','Agg_Supplement_Amt' ]).count().reset_index()

        sorted_df = df_2.sort_values(by=['InstState'], ascending=True)

        # Preparing output as per reporting requirement
        df_dict = sorted_df.to_dict('records')

        # Preparing output as per reporting requirement
        stateList = list(map(lambda x: x['InstState'], df_dict))
        stateListUnique = list(dict.fromkeys(stateList))
        projectList = []
        reportOutput = {}

        for s in stateListUnique:
            projectList = []
            aggDict = {}
            for e in df_dict:
                if (s == e['InstState']):
                    projectList.append({e['ProjectTitle']: e['count']})
                    aggDict = {'Agg_Approved_Outright': e['Agg_Approved_Outright'],
                               'Agg_Award_Outright': e['Agg_Award_Outright'], 'Agg_OriginalAmount': e['Agg_OriginalAmount'],'Agg_Supplement_Amt': e['Agg_Supplement_Amt']}

            if e['InstState'] not in reportOutput.keys():
                reportOutput[s] = {'Projects': projectList, 'aggGrant': aggDict}

        # Print Output
        #print(reportOutput)

        # Writing output to file
        vjson = json.dumps(reportOutput)
        f = open(r"D:\Reports\CountofProject_Agg_Grants.json", "w")
        f.write(vjson)
        f.close()

        con.close()
    except Exception as e:
        print('Error in function countAggGrant - ' + str(e))


if __name__ == '__main__':
    countAggGrant()
