#2/12/2019
#network model

import gurobipy as grb
import csv

#import data usually
breweries = ['RAL','SAV','RICH','CLEV']
distribtion = ['CHR','OXF','CHA']
miles = {('b' , 'd') : [5,5,3,2,3]}
cost = {'b': [5,5,3,2,3]}
demand = {'d': [5,5,3,2,3]}
supply = {'b': [5,5,3,2,3]}

#make model
charbrew = grb.Model('model name')
charbrew.modelSense = grb.GRB.MINIMIZE #objective function goal GRB. = constant
charbrew.update() #need to update after each change to model - after constraints, model, and variables

# make variables - use dictionaries - does positivity constraint for you...
kegs = {}
for b in breweries:
    for d in distribtion:
        kegs[b,d] = charbrew.addVar(obj = cost[b] * miles[b,d], name = 'K(%S,%S)' % (b,d))
charbrew.update() #save variables

#add constraints
myConstrs = {}
#distribution
for d in distribution:
    cName = 'demand_ %S' % d
    myConstrs[cName] = charbrew.addConstr(grb.quicksum(kegs[b,d] for b in breweries) >= demand[d], name = cName)
#supply
for b in breweries:
    xName = 'supply_ %s'
    myConstrs[xName] = charbrew.addConstr(grb.quicksum(kegs[b,d] for d in distribution) >= supply[b], name = xName)

charbrew.update()
charbrew.write('test.lp') #check if constrainst I want, coefficients, indexed properly

charbrew.optimize()

if charbew == grb.GRB.OPTIMAL:
    print('yay')

