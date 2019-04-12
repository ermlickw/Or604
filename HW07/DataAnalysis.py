import pandas as pd
import sqlite3
import pickle

#load raw data to database for storage
def uploadData():
    '''
    this function uploads the raw CSV files to tables in a database called "NFL"
    '''
    #make DB and write output
    conn = sqlite3.connect('NFL.db')
    c = conn.cursor()

    try:
        table_name = 'GAMEVARS' #AWAY_TEAM,HOME_TEAM,WEEK,SLOT,NETWORK,QUAL_POINTS
        c.execute('''CREATE TABLE %s (
                    AWAY text,
                    HOME text,
                    Week int,
                    Slot text,
                    Network text,
                    Quality int)''' %
                    (table_name))
    except:
        c.execute("Delete from %s" % (table_name)) #delete table data if already created

    data = pd.read_csv("GAME_VARIABLES_2018_V1.csv")
    c.executemany('insert into %s values (?,?,?,?,?,?)' % (table_name), data.values.tolist())
    conn.commit()
    #print count
    c.execute('select count(*) from %s'% (table_name))
    result = c.fetchone()
    print('Inserted ' + str(result[0])+ ' records into the '+ table_name+ ' table.')

    try:
        table_name = 'NETWORK' #Week,SLOT,NETWORK
        c.execute('''CREATE TABLE %s (
                    Week int,
                    Slot text,
                    Network text)''' %
                    (table_name))
    except:
        c.execute("Delete from %s" % (table_name)) #delete table data if already created

    data = pd.read_csv("NETWORK_SLOT_WEEK_2018_V1.csv")
    c.executemany('insert into %s values (?,?,?)' % (table_name), data.values.tolist())
    conn.commit()
    #print count
    c.execute('select count(*) from %s'% (table_name))
    result = c.fetchone()
    print('Inserted ' + str(result[0])+ ' records into the '+ table_name+ ' table.')

    try:
        table_name = 'OPPONENTS' #away home
        c.execute('''CREATE TABLE %s (
                    Away text,
                    Home text)''' %
                    (table_name))
    except:
        c.execute("Delete from %s" % (table_name)) #delete table data if already created

    data = pd.read_csv("opponents_2018_V1.csv")
    c.executemany('insert into %s values (?,?)' % (table_name), data.values.tolist())
    conn.commit()
    #print count
    c.execute('select count(*) from %s'% (table_name))
    result = c.fetchone()
    print('Inserted ' + str(result[0])+ ' records into the '+ table_name+ ' table.')

    try:
        table_name = 'TEAMS' #TEAM,CONF,DIV,TIMEZONE,QUALITY
        c.execute('''CREATE TABLE %s (
                    Team text,
                    Conference text,
                    Division text,
                    Timezone int,
                    Quality int)''' %
                    (table_name))
    except:
        c.execute("Delete from %s" % (table_name)) #delete table data if already created
    data = pd.read_csv("TEAM_DATA_2018_v1.csv")
    c.executemany('insert into %s values (?,?,?,?,?)' % (table_name), data.values.tolist())
    conn.commit()
    #print count
    c.execute('select count(*) from %s'% (table_name))
    result = c.fetchone()
    print('Inserted ' + str(result[0])+ ' records into the '+ table_name+ ' table.')




def getCleanData():
    '''
    this function grabs the data from the DB and sends to
    Pandas dataframes for use in the optimization problem
    '''

    conn = sqlite3.connect('NFL.db')

    #load raw data for cleaning
    GV = pd.read_sql(con=conn, sql="select * from GAMEVARS")
    NETWORK = pd.read_sql(con=conn, sql="select * from NETWORK")
    OPPONENTS = pd.read_sql(con=conn, sql="select * from OPPONENTS")
    TEAMDATA = pd.read_sql(con=conn, sql="select * from TEAMS")

    return GV, NETWORK, OPPONENTS, TEAMDATA

def getAwayDict():
    OPPS= getCleanData()[0].loc[:,["AWAY","HOME"]].groupby(['AWAY','HOME']).size().reset_index()
    OPPS = OPPS.groupby('AWAY')['HOME'].apply(list).to_dict()
    return OPPS

def getHomeDict():
    OPPS= getCleanData()[0].loc[:,["AWAY","HOME"]].groupby(['AWAY','HOME']).size().reset_index()
    OPPS = OPPS.groupby('HOME')['AWAY'].apply(list).to_dict()
    OPPS=OPPS.pop('BYE',None) #remove BYE from home teams
    return OPPS