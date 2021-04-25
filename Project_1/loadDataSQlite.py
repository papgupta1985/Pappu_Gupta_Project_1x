import sqlite3
import pandas as pd


'''
Created by          : Pappu gupta
Create Date         : 25-04-2021
Description         : Create database, table and loads data from CSV file
Versions            : 25-04-2021 -  Created 
'''


# create a database connection to the SQLite database specified by db_file

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print('Database created or connection established successfully')
        return conn

    except Exception as e:
        print('Error Creating database  - ' + str(e))

    return conn


# Create Table
def create_table(conn, tableNameDDL):
    try:
        c = conn.cursor()
        c.execute(tableNameDDL)
        print('Table Created successfully')

    except Exception as e:
        print('Error Creating Table - ' + str(e))


# Load Table
def load_table(conn, datafile, tableName='NEH_GRANTS_2010_2019'):
    try:
        # load the data into a Pandas DataFrame
        users = pd.read_csv(datafile)

        # write the data to a sqlite table
        users.to_sql(tableName, conn, if_exists='append', index=False)
        print('Data loaded successfully')

    except Exception as e:
        print('Error loading Table - ' + str(e))


def main(database=r"D:\Database\NEH_GRANTS_DB.db",
         datafile=r"D:\SrcFile\neh-grants-2010-2019-csv-1.csv"):

    tableNameDDL = """CREATE TABLE IF NOT EXISTS NEH_GRANTS_2010_2019 (
                                                                            AppNumber TEXT,
                                                                            ApplicantType TEXT,
                                                                            Institution TEXT,
                                                                            OrganizationType TEXT,
                                                                            InstCity TEXT,
                                                                            InstState TEXT,
                                                                            InstPostalCode TEXT,
                                                                            InstCountry TEXT,
                                                                            CongressionalDistrict INTEGER,
                                                                            Latitude REAL,
                                                                            Longitude REAL,
                                                                            CouncilDate TEXT,
                                                                            YearAwarded INTEGER,
                                                                            ProjectTitle TEXT,
                                                                            Program TEXT,
                                                                            Division TEXT,
                                                                            ApprovedOutright INTEGER,
                                                                            ApprovedMatching INTEGER,
                                                                            AwardOutright INTEGER,
                                                                            AwardMatching INTEGER,
                                                                            OriginalAmount INTEGER,
                                                                            SupplementAmount INTEGER,     
                                                                            BeginGrant TEXT,
                                                                            EndGrant TEXT,
                                                                            ProjectDesc TEXT,
                                                                            ToSupport TEXT,
                                                                            PrimaryDiscipline TEXT,
                                                                            SupplementCount INTEGER,
                                                                            Supplements TEXT,
                                                                            ParticipantCount INTEGER,
                                                                            Participants TEXT,
                                                                            DisciplineCount INTEGER,
                                                                            Disciplines TEXT
                                                                            );
                                                                            """

    # create a database connection
    conn = create_connection(database)

    if conn is not None:
        create_table(conn, tableNameDDL)
        load_table(conn, datafile, 'NEH_GRANTS_2010_2019')
    else:
        print("Error! cannot create the database connection.")


if __name__ == '__main__':
    main()
