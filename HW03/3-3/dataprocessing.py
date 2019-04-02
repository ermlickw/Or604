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
        table_name = 'GoodStores'
        c.execute('''CREATE TABLE %s (
                    StoreNumber int,
                    Store text,
                    Street text,
                    City text,
                    State text,
                    Zip text,
                    Latitude real,
                    Longitude real)''' %
                    (table_name))
    except:
        c.execute("Delete from %s" % (table_name)) #delete table data if already created

    data = pd.read_csv("OR604 Good Dominos Data.csv").values.tolist() #StoreNumber    Store     Street           City State    Zip   Latitude   Longitude
    c.executemany('insert into %s values (?,?,?,?,?,?,?,?)' % (table_name), data)
    conn.commit()
    #print count
    c.execute('select count(*) from %s'% (table_name))
    result = c.fetchone()
    print('Inserted ' + str(result[0])+ ' records into the '+ table_name+ ' table.')

    try:
        table_name = 'DailyDemand'
        c.execute('''CREATE TABLE %s (
                    Date date,
                    StoreNumber int,
                    PizzaSales int)''' %
                    (table_name))
    except:
        c.execute("Delete from %s" % (table_name)) #delete table data if already created

    data = pd.read_csv("OR 604 Dominos Daily Demand.csv").values.tolist() #2011 JAN to 2015 DEC, date store number, sales by day -daily demand
    c.executemany('insert into %s values (?,?,?)' % (table_name), data)
    conn.commit()
    #print count
    c.execute('select count(*) from %s'% (table_name))
    result = c.fetchone()
    print('Inserted ' + str(result[0])+ ' records into the '+ table_name+ ' table.')

    try:
        table_name = 'Distributor'
        c.execute('''CREATE TABLE %s (
                    DCID text,
                    Address text,
                    Latitude real,
                    Longitude real,
                    SupplyCap real,
                    DistCost real)''' %
                    (table_name))
    except:
        c.execute("Delete from %s" % (table_name)) #delete table data if already created

    data = pd.read_csv("Distributor_Data.csv") #Distribution Center IDs                                     Address  Latitude  Longitude Supply Capacity (pizza/week)  Dist Cost ($/mile)
    data.iloc[:,4]=data.iloc[:,4].apply(lambda x: locale.atoi(x))
    data.iloc[:,0]=data.iloc[:,0].apply(lambda x: x.replace(' ','-'))
    c.executemany('insert into %s values (?,?,?,?,?,?)' % (table_name), data.values.tolist())
    conn.commit()
    #print count
    c.execute('select count(*) from %s'% (table_name))
    result = c.fetchone()
    print('Inserted ' + str(result[0])+ ' records into the '+ table_name+ ' table.')



    try:
        table_name = 'Supplier'
        c.execute('''CREATE TABLE %s (
                    Store text,
                    Latitude real,
                    Longitude real,
                    SupplyCap real,
                    UnitCost real)''' %
                    (table_name))
    except:
        c.execute("Delete from %s" % (table_name)) #delete table data if already created

    data = pd.read_csv("Supplier_Data.csv").values.tolist() #Store   Latitude   Longitude Supply Capacity (Unit/week)  Cost per unit ($)
    c.executemany('insert into %s values (?,?,?,?,?)' % (table_name), data)
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
    demandDF = pd.read_sql(con=conn, sql="select * from DailyDemand") #2011 JAN to 2015 DEC, date store number, sales by day -daily demand

    goodDF = pd.read_sql(con=conn, sql="select * from GoodStores") #StoreNumber    Store     Street           City State    Zip   Latitude   Longitude

    distributorDF = pd.read_sql(con=conn, sql="select * from Distributor") #distribution center id, address full, lat long, supply, and distance cost - 16 total

    #if store number in good store but not in demandy then it's new and guess your own values
    newstorelist = list(set(goodDF.iloc[:,0]) - set(demandDF.iloc[:,1]))
    print("\nThere are %s new stores. They include %s \n" % (len(newstorelist), newstorelist))

    #if store number in demand but not good store then it's closed and the store number should be dropped
    closedstorelist = list(set(demandDF.iloc[:,1]) - set(goodDF.iloc[:,0]))
    print("There are %s closed stores. They include %s \n" % (len(closedstorelist), closedstorelist))

    #find average weekly demand for all store numbers and add store, lat and long to that table
        #find total number of days
        # from datetime import date
        # print(max(demandDF.iloc[:,0])) #last date
        # print(min(demandDF.iloc[:,0])) #first date
        # print(len(set((demandDF.iloc[:,0])))) #total days
        # print((date(2015,12,31)-date(2011,1,1)).days +1) == 1826 total days
    demandAVGDF = demandDF.groupby('StoreNumber', as_index=False).agg({'PizzaSales':"mean"}) #daily pizza sale average
        # demandAVGDF.iloc[:,1] *= 1/len(set((demandDF.iloc[:,0]))) alternative way is equivalent
        # print(len(set(demandAVGDF.iloc[:,0])))
        # print(len(set(demandDF.iloc[:,1]))) #check 4862 = 4862
    demandAVGDF.iloc[:,1] *= 7 #changing daily to weekly pizza sale average - using 7 day week avg

    #merge the two dataframes together, keeping all store numbers from both tables
    demandAVGDF.columns = ['StoreNumber', 'AvgWKLYPizzaSales']
    DemandStoreDF = pd.merge(demandAVGDF,goodDF[['StoreNumber','Latitude','Longitude']],
                            on='StoreNumber',how='outer')
    del(demandDF,demandAVGDF,goodDF) #clean up memory since we're done with these

    #remove closed stores
    DemandStoreDF = DemandStoreDF.loc[~DemandStoreDF['StoreNumber'].isin(closedstorelist)]
    #set new stores to the typical average across all stores - could use a predictive model based on location for better accuracy...
    typicalAverage = DemandStoreDF['AvgWKLYPizzaSales'].mean() #1221.82
    DemandStoreDF.loc[DemandStoreDF['StoreNumber'].isin(newstorelist),'AvgWKLYPizzaSales'] = typicalAverage
    #4879 total stores in DF -> =4891-12 check


    #output cleaned data to database.
    c = conn.cursor()

    try:
        table_name = 'WeeklyStoreDemand'
        c.execute('''CREATE TABLE %s (
                    StoreNumber int,
                    AvgWKLYPizzaSales real,
                    Latitude real,
                    Longitude real)''' %
                    (table_name))
    except:
        c.execute("Delete from %s" % (table_name)) #delete table data if already created

    data = DemandStoreDF.values.tolist()
    c.executemany('insert into %s values (?,?,?,?)' % (table_name), data)
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
    DemandStoreDF = pd.read_sql(con=conn, sql="select * from WeeklyStoreDemand") #2011 JAN to 2015 DEC, date store number, sales by day -daily demand

    distributorDF = pd.read_sql(con=conn, sql="select * from Distributor")

    return DemandStoreDF, distributorDF


def createDistanceMatrix(stores,distributions,demandStoreDF,distributorDF):
    '''
    creates distance dictionary between distribution centers and each store. This is only the one way distance, not there and back
    '''
    StoreDistDistances = defaultdict(dict)
    print("\nPlease wait. Creating Distance matrix.\n")
    for store in stores:
            for distribution in distributions:
                LAT1 = demandStoreDF.loc[demandStoreDF['StoreNumber']==store, 'Latitude'].values[0]
                LON1 = demandStoreDF.loc[demandStoreDF['StoreNumber']==store, 'Longitude'].values[0]
                LAT2 = distributorDF.loc[distributorDF['DCID']==distribution, 'Latitude'].values[0]
                LON2 = distributorDF.loc[distributorDF['DCID']==distribution, 'Longitude'].values[0]
                coords_1 = (LAT1,LON1)
                coords_2 = (LAT2,LON2)
                StoreDistDistances[store][distribution]= geopy.distance.vincenty(coords_1, coords_2).mi
    pickle.dump(StoreDistDistances, open("StoreDistDistances", 'wb'))
