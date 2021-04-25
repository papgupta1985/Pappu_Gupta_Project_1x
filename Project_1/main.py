import loadDataSQlite as dbcon
import repListofParticipants as repLofPart
import repAggSupplements as repAggSupp
import repCountAggGrant as repCntAggGrant


if __name__ == '__main__':

    print('*****************Reporting************************')

    print('##### Creating database , table and loading data from CSV file')
    dbcon.main(database=r"D:\Database\NEH_GRANTS_DB.db",
         datafile=r"D:\SrcFile\neh-grants-2010-2019-csv-1.csv")

    print('##### Generating Report 1 - list of participants who are co project directors')
    repLofPart.ListofParticipant(database=r"D:\Database\NEH_GRANTS_DB.db", state='"CA"')

    print('##### Generating Report 2 - Aggregate of the total number of supplements given per year')
    repAggSupp.aggSuppliments(database=r"D:\Database\NEH_GRANTS_DB.db")

    print('##### Generating Report 3 - Count of each project per state with aggregated grants for each state')
    repCntAggGrant.countAggGrant(database=r"D:\Database\NEH_GRANTS_DB.db")

    print('******* 3 - Reports Successfully Generated *****************')


