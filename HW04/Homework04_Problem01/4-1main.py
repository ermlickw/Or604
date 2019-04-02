#3/5

import pandas as pd
import gurobipy as grb
import sqlite3
import pandas as  pd
from dataprocessing import uploadData, cleanData, getCleanData, createDistanceMatrix
import pickle

#upload raw data to DB
uploadData()
# #download data from DB and clean it for this problem by creating new table
cleanData()
# #import cleaned data from DB for use
Mills, DCs = getCleanData()
print(Mills.head())
print(DCs.head())


# #define indicies, data and variabless
mills = Mills['Mill']
dcenters = DCs['DistributionCenter']
distcost = pd.Series(DCs['DistCost'].values,index=dcenters).to_dict()
demand = pd.Series(DCs['WeeklyDemand'].values,index=dcenters).to_dict()
supply = pd.Series(Mills['SupplyCap'].values,index=mills).to_dict()
    #distance matrix - create or load precreated...
createDistanceMatrix(mills,dcenters,Mills,DCs)
miles = pickle.load(open("MillDistDistances",'rb')) #[mill][distributioncenter]
sackcost = pd.Series(Mills['UnitCost'].values,index=mills).to_dict()
startcost=700000



#make model
dominosmodel = grb.Model('Dominos model')
dominosmodel.modelSense = grb.GRB.MINIMIZE #objective function goal GRB. = constant
dominosmodel.update() #need to update after each change to model - after constraints, model, and variables

# # make variables - use dictionaries - does positivity constraint for you...
ship = {} # if mill ships to DC
openmill = {}# if mill is open
for m in mills:
    openmill[m] = dominosmodel.addVar(obj = (startcost), vtype=grb.GRB.BINARY, name = 'OpenMill(%s)' % (m)) #See model formulation PDF
    for d in dcenters:
        ship[m,d] = dominosmodel.addVar(obj = ( 2*(demand[d]/58.1529)*distcost[d]*miles[m][d]/880 + sackcost[m]*demand[d]/58.1529), vtype=grb.GRB.BINARY, name = 'Shipment(%s,%s)' % (m,d)) #See model formulation PDF
dominosmodel.update() #save variables

#add constraints
myConstrs = {}

#d center only served by one mill
for d in dcenters:
    cName = 'Millserving_%s' % d
    myConstrs[cName] = dominosmodel.addConstr(grb.quicksum(ship[m,d] for m in mills) == 1, name=cName)

#supply maximum
for m in mills:
    xName = 'supply_%s' % m
    myConstrs[xName] = dominosmodel.addConstr(grb.quicksum(ship[m,d]*demand[d]/58.1529 for d in dcenters) <= supply[m]*openmill[m], name = xName)


dominosmodel.update()
dominosmodel.write('test.lp') #check if constrainst I want, coefficients, indexed properly
dominosmodel.optimize()
dominosmodel.update()

#if its converged optimal:
if dominosmodel.Status == grb.GRB.OPTIMAL:
    dominosmodel.write('solution.sol') #write the solution to file
    #make DB and write output
    conn = sqlite3.connect('Dominos.db')
    c = conn.cursor()
    try:
        table_name = 'OptimalMillSupplyConfiguration'
        c.execute('''CREATE TABLE %s (
                    Mill text,
                    DC text)''' %
                    (table_name))
    except:
        c.execute("Delete from %s" % (table_name)) #delete table data if already created


    for key, value in ship.items():
        if value.x>0:
            insert = list(key)
            c.executemany('insert into %s values (?,?)' % (table_name), (insert,))
    conn.commit()
    #print count
    c.execute('select count(*) from %s'% (table_name))
    result = c.fetchone()
    print('Inserted ' + str(result[0])+ ' records into the '+ table_name+ ' table.')
