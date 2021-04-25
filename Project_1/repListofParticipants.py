import pandas as pd
import loadDataSQlite as dbcon
import json

'''
Created by          : Pappu gupta
Create Date         : 25-04-2021
Description         : Returns a list of participants who are co project directors who
                      worked on projects within a certain state, the state will be provided as a parameter input 
Versions            : Created 
'''

pd.set_option('display.width', 500)
pd.set_option('display.max_columns', 10)
pd.set_option('mode.chained_assignment', None)


# Returns a list of participants who are co project directors. default state = CA
def ListofParticipant(database=r"D:\Database\NEH_GRANTS_DB.db", state='"IA"'):
    try:
        # get database connection
        con = dbcon.create_connection(database)

        # query preparation
        query = f"""select Participants  from NEH_GRANTS_2010_2019 where InstState = {state}"""

        # load data into dataframe
        df = pd.read_sql_query(query, con)

        # Explode Co Project Director and Project Director
        df1 = df.assign(Participants=df['Participants'].str.split(';')).explode('Participants')

        # Filter rows where Participants = 'Co Project Director'
        df2 = df1[df1['Participants'].str.contains('Co Project Director', na=False)]

        # Removing [Co Project Director]
        df2['Participants'] = df2['Participants'].apply(lambda x: x.replace(' [Co Project Director]', ''))

        # Convert DataFrame to Dictionary
        dfdict = df2.to_dict('records')

        # Preparing output as per reporting requirement
        l1 = []
        for de in dfdict:
            l1.append(de['Participants'].strip())

        reportOutput = {state.replace('"', ''): l1}

        # Print Output
        # print(reportOutput)

        # Writing output to file
        vjson = json.dumps(reportOutput)
        f = open(r"D:\Reports\List_of_Participants.json", "w")
        f.write(vjson)
        f.close()

        con.close()
    except Exception as e:
        print('Error in function ListofParticipant - ' + str(e))


if __name__ == '__main__':
    ListofParticipant()
