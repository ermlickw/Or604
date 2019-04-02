#2/12/2019
#network model - this imports data from the 2 CSVs

import gurobipy as grb
import sqlite3
import pandas as pd

#import data usually
data=pd.read_csv("Data.csv")
floordata = pd.read_csv("Floors.csv")
hours = [835]
#creating english index for easy reference
machines = data.iloc[0:,0].values
data=data.set_index('NAME')
floors=floordata.iloc[0:,1].values
floordata=floordata.set_index('Floor')

print(machines)
#make model
slotsmodel = grb.Model('model name')
slotsmodel.modelSense = grb.GRB.MAXIMIZE #objective function goal GRB. = constant
slotsmodel.update() #need to update after each change to model - after constraints, model, and variables

# make variables - use dictionaries - does positivity constraint for you...
slots = {}
for f in floors:
    for slot in machines:
        slots[slot,f] = slotsmodel.addVar(obj = data.loc[slot]['EXPREV']-data.loc[slot]['OCOST'], name = 'Slot(%s,%s)' % (slot,f)) #obj=objective function coefficient for decision variables
slotsmodel.update() #save variables

#add constraints
myConstrs = {}

#floor space:
for f in floors:
    cName = 'floorspace_%s' % f
    print(cName)
    myConstrs[cName] = slotsmodel.addConstr(grb.quicksum(slots[slot,f] * data.loc[slot]['AREA'] for slot in machines) <= floordata.loc[f], name=cName)

#hours:
cName = 'hours'
myConstrs[cName] = slotsmodel.addConstr(grb.quicksum(slots[slot,f] * data.loc[slot]['MAINTENANCE'] for slot in machines for f in floors) <= hours[0], name=cName)

#OnHandLimits:
for slot in machines:
    cName = 'onhand_%s' % slot
    myConstrs[cName] = slotsmodel.addConstr(grb.quicksum(slots[slot,f] for f in floors) <= data.loc[slot]['HAND'], name=cName)

slotsmodel.update()
slotsmodel.write('test.lp') #check if constrainst I want, coefficients, indexed properly


slotsmodel.optimize()
slotsmodel.update()


if slotsmodel.Status == grb.GRB.OPTIMAL:
    slotsmodel.write('solution.sol') #write the solution to file

    #make DB and write output
    conn = sqlite3.connect('Casino.db')
    c = conn.cursor()
    try:
        table_name = 'Casino_Machine_Floor_Count'
        c.execute('''CREATE TABLE %s (
                    Machine text,
                    Floor text,
                    Count text)''' %
                    (table_name))
    except:
        c.execute("Delete from %s" % (table_name)) #delete table data if already created


    for key, value in slots.items():
        if value.x>0:
            insert = list(key) + [value.x]
            c.executemany('insert into %s values (?,?,?)' % (table_name), (insert,))
    conn.commit()
    #print count
    c.execute('select count(*) from %s'% (table_name))
    result = c.fetchone()
    print('Inserted ' + str(result[0])+ ' records into the '+ table_name+ ' table.')




    #hw 1 only output the values greater than 0
    # B    D    KEGS
    # SAV   CHR  3  text, datacsv, database csv - single zip file - relative referencing
