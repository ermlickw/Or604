'''
Script to facilitate making seeds for solving the full problem. This script finds games which must be zero or one based on 
(1) the given constraints and 
(2) running the model quickly to determine infesible games
'''
import gurobipy as grb
import time
import pandas as pd



def set_constrained_variables():
    '''
    This function sets the games which must be zero based on the constraints to zero (i.e. zero upper and lower bound)
    '''

    myConstrs = NFLmodel.getConstrs()
    for c in myConstrs:
        SoftlinkingConstr = False
        if c.sense =='<'and c.RHS == 0:
            row = NFLmodel.getRow(c)
            #check if the row contains a linking term or penalty term
            for r in range(row.size()):
                if 'GO_' not in row.getVar(r).varName:
                    SoftlinkingConstr = True
                    break
            # if it doesnt contain one of those terms then set all the variables in that constraint to zero
            if not SoftlinkingConstr:
                for r in range(row.size()):
                    row.getVar(r).lb = 0
                    row.getVar(r).ub = 0
                    NFLmodel.update()
    return
                

def get_variables():
    '''
    makes two dictionarys keyed over cleaned up tuples of the game variables:
    (1) freevars - are the gurobi objects of the variables which are not yet assigned
    (2) var_status contains a tuple indicating the bounds of the variables
    '''
    free_vars = {}
    var_status = {}
    temp=[]
    myVars = NFLmodel.getVars()
    for v in myVars:
        if 'GO' == v.varName[:2]:
            temp = v.varName.split('_')
            if 'PRIME' in temp:
                free_vars[tuple(temp[1:])] = v
                var_status[tuple(temp[1:])]=(v.lb,v.ub)
    free_vars = cleanfreevars(free_vars,var_status)
    print(len(var_status))
    print(len(free_vars) )
    return free_vars, var_status

def cleanfreevars(free_vars,var_status):
    '''
    this kills any variables in freevars which now have fixed bounds as recorded in var_status
    '''
    for v in var_status:
        if var_status[v][0]==var_status[v][1]:
            if v in free_vars:
                free_vars.pop(v,None)
    return free_vars

def fix_impossible_games(free_vars,var_status):
    start=time.time()
    NFLmodel.setParam('OutputFlag', False )
    NFLmodel.setParam('TimeLimit',10)
    NFLmodel.write('updated.lp') #first time only
    
    print('Starting...')
    stop = False
    while not stop:
        stop = True
        end = time.time()
        print(str(len(free_vars))+ ' free variables remaining. Runtime = ' + str(round((end-start)/60,2)) + ' minutes.')
        for v in free_vars:
            free_vars[v].lb = 1
            NFLmodel.update()
            NFLmodel.optimize()
            if NFLmodel.Status == grb.GRB.INFEASIBLE:
                var_status[v] = (0,0)
                free_vars[v].lb = 0
                free_vars[v].ub = 0
                stop = False
                print(str(v) + ' is fixed to zero')
            else:
                free_vars[v].lb = 0
                print(str(v) + ' is free')
                NFLmodel.update()
        free_vars = cleanfreevars(free_vars,var_status)
        NFLmodel.write('updated.lp')
        print(str(len(free_vars))+ ' free variables remaining. Runtime =  ', str(round((end-start)/60,2))+ ' minutes.')



if __name__ == "__main__":
    #load model
    NFLmodel=grb.read('OR604 Model File v2.lp')
    #load constraints
    set_constrained_variables()
    free_vars, var_status = get_variables()
    fix_impossible_games(free_vars,var_status)
    write = pd.DataFrame.from_dict(var_status,orient="index")
    write.to_csv("GameBounds.csv")
    NFLmodel.write('updated.lp')