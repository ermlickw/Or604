'''
OR604 HW00
William Ermlick
1/13/2018

Sources:
https://docs.python.org/2/library/sqlite3.html
https://stackoverflow.com/questions/17044259/python-how-to-check-if-table-exists/17044893
https://stackoverflow.com/questions/19585280/convert-a-row-in-pandas-into-list
https://stackoverflow.com/questions/46028456/import-csv-files-into-sql-database-using-sqlite-in-python
https://stackoverflow.com/questions/4913349/haversine-formula-in-python-bearing-and-distance-between-two-gps-points
'''
import os
import pandas as pd
import numpy as np
import seaborn as sns
import sqlite3
import pickle
from datetime import datetime
from math import radians, cos, sin, asin, sqrt
from collections import defaultdict

def Problem_1():
    '''
    Get the basic station information into the database.  Load the station file
    “Capital_Bikeshare_Terminal_Locations.csv” into a table in the database.
    Your solution should check to see if the data table exists and creates the
    table if it does not exist.  Your solution should also output to the screen
    the total number of records entered into the data table.  This problem gives
    you the skills required to connect to a database, create tables, import data
    from a CSV and use Python abstract data structures.  HINTS:  use the
    executemany command in SQLite to load the data into the database.  Recommend
    you use the lists to temporarily store the data in memory before loading it
    into the database (as opposed to using a dictionary – which seems to be a
    bit more difficult to implement correctly for some people).  Use the
    following date-time format when loading the data into the SQLite database:
    YYYY-MM-DD HH:SS.  The dashes are important because that is the only
    format that SQLite knows and recognizes inherently as a date.
    '''
    conn = sqlite3.connect('BikeShare.db')
    c = conn.cursor()
    table_name = 'Capital_Bike_Share_Locations'

    # Create table if it doesnt exist
    existedalready = False
    try:
        c.execute('''CREATE TABLE %s (
                    Object_ID real,
                    ID real,
                    Address text,
                    Terminal int,
                    Latitude real,
                    Longitude real,
                    Installed text,
                    Locked text,
                    Install_Date date,
                    Removal_Date date,
                    Temporary_Install text,
                    Number_Of_Bikes int,
                    Number_Of_Empty_Docks int,
                    X real,
                    Y real,
                    SE_Anno_CAD_Data real)''' %
                    (table_name))
    except sqlite3.OperationalError as e: #if it already exists, log that
        message = e.args[0]
        if message.startswith("table " + table_name + " already exists"):
            existedalready = True
        pass

    if existedalready == False:
        # populate the DB if it didn't exist and we just made it
        data = pd.read_csv(table_name+'.csv')

        #convert DF to list of rows formatted as lists
        rows=data.values.tolist()
        #insert each row into the DF
        c.executemany('insert into %s values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)' % (table_name), rows)
        conn.commit()
        #print count
        c.execute('select count(*) from %s'% (table_name))
        result = c.fetchone()
        print('Inserted ' + str(result[0])+ ' records into the '+ table_name+ ' table.')
    else:
        print(table_name+" already exists and is populated.")
    return


def Problem_2():
    '''
    Get the trip data into the database.  Iterate over all the data files that contain
    trip history (these are the 24 data files contained in the zipped directory) and
    load them into a single data table in a database.  Your solution should check for
    the existence of the data table that holds the bike data (and create it if it does
    not exist), and load the contents of the files into the data table.  Your solution
    should also output the number of records transferred from each data set as well
    as return the total number of records that have been loaded into the data table.
    This problem gives you the skills required to iterate over multiple data sets,
    insert data into an existing table, and creating tables if one does not exist.
    HINTS:  use the same hints for Problem 1 and I recommend that you extract
    the data from the zipped archive into a directory as opposed to trying to
    access the files from within the archive.  Iterate over the 24 files do not
     type all 24 file names into your routine.
    '''
    conn = sqlite3.connect('BikeShare.db')
    c = conn.cursor()
    table_name = 'Capital_Bike_Share_Data'
    datafolder = os.path.realpath("Capital_BikeShare_Data")
    # Create table if it doesnt exist
    existedalready = False
    try:
        c.execute('''CREATE TABLE %s (
                    TRIP_DURATION real,
                    START_DATE date,
                    START_STATION int,
                    STOP_DATE date,
                    STOP_STATION int,
                    BIKE_ID int,
                    USER_TYPE text)''' %
                    (table_name))
    except sqlite3.OperationalError as e: #if it already exists, log that
        message = e.args[0]
        if message.startswith("table " + table_name + " already exists"):
            existedalready = True
        pass

    if existedalready == False:
        # populate the DB if it didn't exist and we just made it
        for root, dirs, files in os.walk(datafolder): #walk it
            for file in files:
                if file.endswith(".csv"):
                    data = pd.read_csv(os.path.join(datafolder, file))
                    # #format date columns as required
                    # print(data['STOP_DATE'].apply(lambda x: datetime.datetime.strftime(str(x),'%Y-%m-%d %H:%S')))
                    #convert DF to list of rows formatted as lists
                    rows=data.values.tolist()
                    #insert each row into the DF
                    c.executemany('insert into %s values (?,?,?,?,?,?,?)' % (table_name), rows)
                    conn.commit()
                    #print status
                    print('Inserted ' + str(c.rowcount)+ ' records into the '+ table_name+ ' table from '+ file+'.')
        #print final status
        c.execute('select count(*) from %s'% (table_name))
        result=c.fetchone()
        print('Inserted ' + str(result[0])+ ' records into the '+ table_name+ ' table from all CSV files in '+ datafolder+'.')


def Problem_3(lat1, lon1, lat2, lon2, Miles):
    '''
    Write a routine (or call an existing module) that takes as its arguments the
     LAT/LON for any two points and whether distance should be calculated as miles
      or kilometers.  The output of the routine should be the distance between the
      two points (in the specified unit of measure).  This problem exercises your
      ability to make routines that take in required arguments and passes out a
      formatted response.
    '''
    if Miles:
        R = 3959 #radius of the earth in miles
    else:
        R= 6371  #radius of the earth in kilometers

    #implement haversine function:
     # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    return c * R


def Problem_4(miles=True):
    '''
    Create a routine that returns a dictionary that has as its keys all pairs of
     bike terminals and the distance between those two locations.  Use your solution
      to problem 3 for this problem.  Your solution should include calling a query
      in your SQLite database that provides you the data you need to calculate the
       distance between all station pairs.  This problem exercises your ability to
       manipulate data sets within a database as well as use dictionaries to save data.
       Hint:  This means you have to make a join between two tables.  You could do a
       Cartesian join or you could iterate over all bike terminals and then find the
       distance from terminal to all other terminals.
    '''
    conn = sqlite3.connect('BikeShare.db')
    c = conn.cursor()
    c.execute('''select DISTINCT Terminal from Capital_Bike_Share_Locations''')
    StationDistances = defaultdict(dict)
    allstops = [row[0] for row in c]
    print("Building dictonary for Problem 4. Please wait.")
    for k in allstops:
        for j in allstops:
            k=str(k)
            j=str(j)
            LAT1 = c.execute('''select Latitude from Capital_Bike_Share_Locations where Terminal = "%s"''' % (k)).fetchone()[0]
            LON1 = c.execute('''select Longitude from Capital_Bike_Share_Locations where Terminal = "%s"''' % (k)).fetchone()[0]
            LAT2 = c.execute('''select Latitude from Capital_Bike_Share_Locations where Terminal = "%s"''' % (j)).fetchone()[0]
            LON2 = c.execute('''select Longitude from Capital_Bike_Share_Locations where Terminal = "%s"''' % (j)).fetchone()[0]
            StationDistances[k][j]=Problem_3(LAT1,LON1,LAT2,LON2,Miles=miles) # assume miles
    pickle.dump(StationDistances, open("StationDistances", 'wb'))
    return StationDistances

def Problem_5(StationDistances, Terminal, Threshold_Distance):
    '''
    Create a routine that takes as its argument a dictionary, a Bikeshare terminal,
    and a distance and returns a list of all stations that are within the specified
    distance of the specified docking station.  This problem tests your ability to
    write a routine that takes in arguments, passes out results, and tests your ability
    to filter off of keys in a dictionary.
    '''
    withindistance=[]
    for item in StationDistances[Terminal]:
        if StationDistances[Terminal][item] < Threshold_Distance:
            withindistance.append(item)
    return print(withindistance)

def Problem_6(station1,station2,startdate,enddate):
    '''
    Create a routine that takes as its argument any two BikeShare stations and a
    start and end date and returns the total number of trips made by riders between
    those two stations over the period of time specified by the start and stop date.
    This problem tests your ability to write a select statement on a table in a
    database and return the results from a select query.
    '''
    conn = sqlite3.connect('BikeShare.db')
    c = conn.cursor()
    c.execute('''select COUNT(*) from Capital_Bike_Share_Data
                 WHERE (START_STATION = %s OR START_STATION = %s)
                 AND  (STOP_STATION=%s OR STOP_STATION = %s)
                 AND START_DATE >  '%s'
                 AND STOP_DATE < '%s' ''' % (station1,station2,
                                            station1,station2,
                                            startdate,enddate))
    result = [row[0] for row in c]
    print(result)

    return


if __name__ == '__main__':
    Problem_1()
    Problem_2()
    StationDistances = Problem_4(miles=True)
    try:
        Problem_5(StationDistances,'32221', .5)
    except :
        StationDistances = pickle.load(open("StationDistances",'rb'))
        Problem_5(StationDistances,'32221', .5)
    Problem_6('31100','31101','2010-12-31 23:49:00','2013-12-31 22:19:00')
