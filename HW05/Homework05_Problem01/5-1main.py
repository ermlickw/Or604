import pandas as pd
import gurobipy as grb


#prep and clean data
feedstockdf = pd.read_csv("feedstock.csv")
demanddf = pd.read_csv("demand_price.csv")
productiondf = pd.read_csv("production.csv")
productiondf.columns = range(14)
productiondf=productiondf.iloc[:,:13]
productiondf.columns=productiondf.columns.astype(str)
productiondf = productiondf.set_index('0')
productiondf.columns=productiondf.columns.astype(int)


# # #define indicies, data and variabless
months = demanddf['Month'].values
demand = pd.Series(demanddf.iloc[:,1].values,index=months).to_dict()
saleprice = pd.Series(demanddf.iloc[:,2].apply(lambda x: float(x[2:].strip())).values,index=months).to_dict()
calfprice = pd.Series(feedstockdf.iloc[:,1].apply(lambda x: round(float(x[2:].strip()))).values, index=months).to_dict()
production = productiondf.transpose().to_dict() #[month calved][productionmonth]
daysinmonths = pd.Series([31,28,31,30,31,30,31,31,30,31,30,31],index=months).to_dict()


#make model
cowmodel = grb.Model('Cow model')
cowmodel.modelSense = grb.GRB.MINIMIZE #objective function goal GRB. = constant
cowmodel.update() #need to update after each change to model - after constraints, model, and variables

# # make variables - use dictionaries - does positivity constraint for you...

cowscalved = {}# cows calved in month m
excess = {}
shortage = {}
for m in months:
    cowscalved[m] = cowmodel.addVar(vtype=grb.GRB.INTEGER, name = 'CowsCalved(%s)' % (m)) #See model formulation PDF
    excess[m] = cowmodel.addVar( name = 'Excess(%s)' % (m)) #See model formulation PDF
    shortage[m] = cowmodel.addVar(name = 'Shortage(%s)' % (m)) #See model formulation PDF
cowmodel.update() #save variables


#set objective function
cowmodel.setObjective( grb.quicksum(cowscalved[m]*calfprice[m] for m in months) +
                       grb.quicksum(shortage[d]*saleprice[d]  for d in months) +
                       grb.quicksum(0.2*excess[d]*saleprice[d] for d in months) ,grb.GRB.MINIMIZE)
#add constraints
myConstrs = {}

# meet demand
for d in months:
    cName = 'MonthlyDemand_%s' % d
    myConstrs[cName] = cowmodel.addConstr(grb.quicksum(cowscalved[m]*production[m][d]*daysinmonths[m] for m in months) -excess[d]+shortage[d] == demand[d], name=cName)

cowmodel.update()
cowmodel.write('test.lp') #check if constrainst I want, coefficients, indexed properly
cowmodel.optimize()
cowmodel.update()

#if its converged optimal:
if cowmodel.Status == grb.GRB.OPTIMAL:
    cowmodel.write('solution.sol') #write the solution to file

    for key, value in cowscalved.items():
            print(value)
