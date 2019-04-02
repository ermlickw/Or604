#2/12/2019
#network model

import gurobipy as grb
import sqlite3

#import data manually
machines = ['NB','DR','DOLR','GP','HDD','ML','BJK']
Attributes = ['HAND',"AREA","EXPREV","OCOST",'MAINTENANCE']
Hours = [835]
FSpace= {
    'floor1':750,
    'floor2':1000,
    'floor3':550,
    'floor4':700
    }
Traits ={
    ('HAND','NB'):200,
    ('HAND','DR'):150,
    ('HAND','DOLR'):100,
    ('HAND','GP'):250,
    ('HAND','HDD'):125,
    ('HAND','ML'):100,
    ('HAND','BJK'):250,
    ('AREA','NB'):2,
    ('AREA','DR'):2.5,
    ('AREA','DOLR'):6,
    ('AREA','GP'):2.75,
    ('AREA','HDD'):3.5,
    ('AREA','ML'):4,
    ('AREA','BJK'):2.75,
    ('EXPREV','NB'):2000,
    ('EXPREV','DR'):3500,
    ('EXPREV','DOLR'):4500,
    ('EXPREV','GP'):750,
    ('EXPREV','HDD'):3000,
    ('EXPREV','ML'):2500,
    ('EXPREV','BJK'):3000,
    ('OCOST','NB'):800,
    ('OCOST','DR'):400,
    ('OCOST','DOLR'):1500,
    ('OCOST','GP'):200,
    ('OCOST','HDD'):1000,
    ('OCOST','ML'):500,
    ('OCOST','BJK'):500,
    ('MAINTENANCE','NB'):0.67,
    ('MAINTENANCE','DR'):1,
    ('MAINTENANCE','DOLR'):2,
    ('MAINTENANCE','GP'):1.1,
    ('MAINTENANCE','HDD'):0.67,
    ('MAINTENANCE','ML'):0.5,
    ('MAINTENANCE','BJK'):0.75
}


#make model
slotsmodel = grb.Model('model name')
slotsmodel.modelSense = grb.GRB.MAXIMIZE #objective function goal GRB. = constant
slotsmodel.update() #need to update after each change to model - after constraints, model, and variables

# make variables - use dictionaries - does positivity constraint for you...
slots = {}
for f in FSpace:
    for slot in machines:
        slots[slot,f] = slotsmodel.addVar(obj = (Traits['EXPREV',slot]-Traits['OCOST',slot]), name = 'Slot(%s,%s)' % (slot,f)) #obj=objective function coefficient for decision variables
slotsmodel.update() #save variables

#add constraints
myConstrs = {}

#floor space:
for f in FSpace:
    cName = 'floorspace_%s' % f
    myConstrs[cName] = slotsmodel.addConstr(grb.quicksum(slots[slot,f] * Traits['AREA',slot] for slot in machines) <= FSpace[f], name=cName)

#hours:
cName = 'hours'
myConstrs[cName] = slotsmodel.addConstr(grb.quicksum(slots[slot,f] * Traits['MAINTENANCE',slot] for slot in machines for f in FSpace) <= Hours[0], name=cName)

#OnHandLimits:
for slot in machines:
    cName = 'onhand_%s' % slot
    myConstrs[cName] = slotsmodel.addConstr(grb.quicksum(slots[slot,f] for f in FSpace) <= Traits['HAND',slot], name=cName)

slotsmodel.update()
slotsmodel.write('test.lp') #check if constraint I want, coefficients, indexed properly


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
