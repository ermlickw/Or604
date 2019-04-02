#2/12/2019
#network model

import gurobipy as grb
import sqlite3
import pandas as  pd
from dataprocessing import uploadData, cleanData, getCleanData, createDistanceMatrix
import pickle

#upload raw data to DB
uploadData()
#download data from DB and clean it for this problem by creating new table
cleanData()
#import cleaned data from DB for use
demandStoreDF, distributorDF = getCleanData()

#define indicies, data and variabless
stores = demandStoreDF['StoreNumber']
distributions = distributorDF['DCID']
cost = pd.Series(distributorDF['DistCost'].values,index=distributions).to_dict()
demand = pd.Series(demandStoreDF['AvgWKLYPizzaSales'].values,index=stores).to_dict()
supply = pd.Series(distributorDF['SupplyCap'].values,index=distributions).to_dict()
    #distance matrix - create or load precreated...
createDistanceMatrix(stores,distributions,demandStoreDF,distributorDF)
StoreDistDistances = pickle.load(open("StoreDistDistances",'rb')) #[store][distributioncenter]

#make model
dominosmodel = grb.Model('Dominos model')
dominosmodel.modelSense = grb.GRB.MINIMIZE #objective function goal GRB. = constant
dominosmodel.update() #need to update after each change to model - after constraints, model, and variables

# # make variables - use dictionaries - does positivity constraint for you...
doughs = {}
for d in distributions:
    for s in stores:
        doughs[d,s] = dominosmodel.addVar(obj = 2*cost[d]*StoreDistDistances[s][d]/9000, name = 'Doughs(%s,%s)' % (d,s)) #two time the distance since the truck comes there and back
dominosmodel.update() #save variables

#add constraints
myConstrs = {}

#demand minimum
for s in stores:
    cName = 'demand_store_%s' % s
    myConstrs[cName] = dominosmodel.addConstr(grb.quicksum(doughs[d,s] for d in distributions) >= demand[s], name=cName)

#supply maximum
for d in distributions:
    xName = 'supply_%s' % d
    myConstrs[xName] = dominosmodel.addConstr(grb.quicksum(doughs[d,s] for s in stores) <= supply[d], name = xName)


dominosmodel.update()
dominosmodel.write('test.lp') #check if constrainst I want, coefficients, indexed properly
dominosmodel.optimize()
dominosmodel.update()


if dominosmodel.Status == grb.GRB.OPTIMAL:
    dominosmodel.write('solution.sol') #write the solution to file

    #make DB and write output
    conn = sqlite3.connect('Dominos.db')
    c = conn.cursor()
    try:
        table_name = 'OptimalSupplyConfiguration'
        c.execute('''CREATE TABLE %s (
                    DCID text,
                    StoreNumber int,
                    DoughCount real)''' %
                    (table_name))
    except:
        c.execute("Delete from %s" % (table_name)) #delete table data if already created


    for key, value in doughs.items():
        if value.x>0:
            insert = list(key) + [value.x]
            c.executemany('insert into %s values (?,?,?)' % (table_name), (insert,))
    conn.commit()
    #print count
    c.execute('select count(*) from %s'% (table_name))
    result = c.fetchone()
    print('Inserted ' + str(result[0])+ ' records into the '+ table_name+ ' table.')
