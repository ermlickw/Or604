import pandas as pd
import sqlite3
import geopy.distance
from collections import defaultdict
import pickle
import locale
locale.setlocale( locale.LC_ALL, 'en_US.UTF-8' )
#load raw data to database for storage
def uploadData():
    '''
    this function uploads the raw CSV files to tables in a database called "Dominos"
    '''
    #make DB and write output
    conn = sqlite3.connect('Dominos.db')
    c = conn.cursor()

    try:
        table_name = 'Mills'
        c.execute('''CREATE TABLE %s (
                    Mill text,
                    Latitude real,
                    Longitude real,
                    SupplyCap real,
                    UnitCost real)''' %
                    (table_name))
    except:
        c.execute("Delete from %s" % (table_name)) #delete table data if already created

    data = pd.read_csv("Ardent_Mills_Data.csv")
    data.iloc[:,3]=data.iloc[:,3].apply(lambda x: locale.atoi(x)) #Store,Latitude,Longitude,Supply Capacity (Unit/week),Cost per unit ($)
    data.iloc[:,0]=data.iloc[:,0].apply(lambda x: x.replace(' ',''))
    c.executemany('insert into %s values (?,?,?,?,?)' % (table_name), data.values.tolist())
    conn.commit()
    #print count
    c.execute('select count(*) from %s'% (table_name))
    result = c.fetchone()
    print('Inserted ' + str(result[0])+ ' records into the '+ table_name+ ' table.')

    try:
        table_name = 'DailyDemand'
        c.execute('''CREATE TABLE %s (
                    StoreID int,
                    DailyDemand int,
                    DistributionCenter text)''' %
                    (table_name))
    except:
        c.execute("Delete from %s" % (table_name)) #delete table data if already created

    data = pd.read_csv("average_daily_demand.csv") #STOREID,average daily demand,Distribution center
    c.executemany('insert into %s values (?,?,?)' % (table_name), data.values.tolist())
    conn.commit()
    #print count
    c.execute('select count(*) from %s'% (table_name))
    result = c.fetchone()
    print('Inserted ' + str(result[0])+ ' records into the '+ table_name+ ' table.')

    try:
        table_name = 'Distributor'
        c.execute('''CREATE TABLE %s (
                    DistributionCenter text,
                    Address text,
                    Latitude real,
                    Longitude real,
                    SupplyCap real,
                    DistCost real)''' %
                    (table_name))
    except:
        c.execute("Delete from %s" % (table_name)) #delete table data if already created

    data = pd.read_csv("Distributor_Data.csv") #Distribution Center IDs,Address,Latitude,Longitude,Supply Capacity (pizza/week),Dist Cost ($/mile)
    data.iloc[:,4]=data.iloc[:,4].apply(lambda x: locale.atoi(x))
    data.iloc[:,0]=data.iloc[:,0].apply(lambda x: x.replace(' ',''))
    c.executemany('insert into %s values (?,?,?,?,?,?)' % (table_name), data.values.tolist())
    conn.commit()
    #print count
    c.execute('select count(*) from %s'% (table_name))
    result = c.fetchone()
    print('Inserted ' + str(result[0])+ ' records into the '+ table_name+ ' table.')



def cleanData():
    '''
    this function cleans the data stored the the database and produces A new table useful
    for this problem called "WeeklyStoreDemand" This is done in Pandas.
    '''
    conn = sqlite3.connect('Dominos.db')

    #load raw data for cleaning
    demandDF = pd.read_sql(con=conn, sql="select * from DailyDemand") #StoreNumber    Store     Street           City State    Zip   Latitude   Longitude
    distributorDF = pd.read_sql(con=conn, sql="select * from Distributor") #distribution center id, address full, lat long, supply, and distance cost - 16 total

    ## find total weekly demand for each DC from dailydemand table and add it to distributor table as new columns

    demandSUMDF = demandDF.groupby('DistributionCenter', as_index=False).agg({'DailyDemand':"sum"}) #daily dough need by DC
    demandSUMDF.iloc[:,1] *= 7 #changing daily to weekly demand
    demandSUMDF.columns = ['DistributionCenter','WeeklyDemand'] #rename to weekly
    DemandDistDF = pd.merge(demandSUMDF,distributorDF,on='DistributionCenter',how='inner') #merge with distributor data
    del(demandSUMDF,distributorDF,demandDF)#clean memory

    #output cleaned data to database.
    c = conn.cursor()

    try:
        table_name = 'WeeklyDistributorDemand'
        c.execute('''CREATE TABLE %s (
                    DistributionCenter text,
                    WeeklyDemand int,
                    Address text,
                    Latitude real,
                    Longitude real,
                    SupplyCap real,
                    DistCost real)''' %
                    (table_name))
    except:
        c.execute("Delete from %s" % (table_name)) #delete table data if already created

    data = DemandDistDF.values.tolist()
    c.executemany('insert into %s values (?,?,?,?,?,?,?)' % (table_name), data)
    conn.commit()
    #print count
    c.execute('select count(*) from %s'% (table_name))
    result = c.fetchone()
    print('Inserted ' + str(result[0])+ ' records into the '+ table_name+ ' table.')


def getCleanData():
    '''
    this function grabs the data from WeeklyStoreDemand and DistributionCenter
    and exports two Pandas dataframes for use in the optimization problem
    '''

    conn = sqlite3.connect('Dominos.db')

    #load raw data for cleaning
    Mills = pd.read_sql(con=conn, sql="select * from Mills")
    DCs = pd.read_sql(con=conn, sql="select * from WeeklyDistributorDemand")

    return Mills, DCs


def createDistanceMatrix(mills,dcenters,Mills,DCs):
    '''
    creates distance dictionary between distribution centers and each store. This is only the one way distance, not there and back
    '''
    MillDistDistances = defaultdict(dict)
    print("\nPlease wait. Creating Distance matrix.\n")
    for mill in mills:
            for dcenter in dcenters:
                LAT1 = Mills.loc[Mills['Mill']==mill, 'Latitude'].values[0]
                LON1 = Mills.loc[Mills['Mill']==mill, 'Longitude'].values[0]
                LAT2 = DCs.loc[DCs['DistributionCenter']==dcenter, 'Latitude'].values[0]
                LON2 = DCs.loc[DCs['DistributionCenter']==dcenter, 'Longitude'].values[0]
                coords_1 = (LAT1,LON1)
                coords_2 = (LAT2,LON2)
                MillDistDistances[mill][dcenter]= geopy.distance.vincenty(coords_1, coords_2).mi
    pickle.dump(MillDistDistances, open("MillDistDistances", 'wb'))
