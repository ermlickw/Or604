import gurobipy as grb
import time
import pandas as pd
import multiprocessing as mp 
import traceback
import random
import time
import colorama
from colorama import Fore, Back, Style
colorama.init()

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

def varProb(iq,oq):
    NFLR = grb.read('OR604 Model File v2.lp')
    NFLR.setParam('OutputFlag', False )
    NFLR.setParam('timelimit',10)
    NFLR.setParam('threadlimit',1)
    
    while True:
        try:
            task = iq.get()
            try:
                if task[0] == None:
                    break
                start_time = time.localtime()
                ##operate on the variable
                myVar = NFLR.getVarByName(task)
                myVar.lb = 1
                NFLR.update()
                NFLR.optimize()
                if NFLR.Status == grb.GRB.INFEASIBLE:
                    myVar.lb = 0
                    myVar.ub = 0
                    mymessage = task + ' -- is infeasible Time: ' + str(time.mktime(time.localtime())-time.mktime(start_time))
                else:
                    myVar.lb = 0
                    mymessage = task + ' -- is good. Time: '  + str(time.mktime(time.localtime())-time.mktime(start_time))
                NFLR.update()
                oq.put((1,mymessage))
            except:
                oq.put((2,traceback.format_exc()))
        except:
            time.sleep(4)
    return

def MyHandler(free_vars,pool_size, var_status, start_time, NFLmodel):

    def populate_queue(freevars,inputqueue, counter):
        for v in freevars:
            if freevars[v].lb != freevars[v].ub:
                varname = 'GO_' + '_'.join(list(v))
                inputqueue.put(varname) # dont have to pickle just needs to be pickeable
                counter+=1
        print('(MASTER): COMPLETED LOADING QUEUE WITH TASKS WITH A TOTAL RUN TIME OF %s' % str(time.mktime(time.localtime())-time.mktime(start_time)))
        return inputqueue, counter
    
    def killswitch(pool_size, iq):
        for i in range(pool_size*2):
            iq.put((None,None))
        print('(MASTER): COMPLETED LOADING QUEUE WITH NONES WITH A TOTAL RUN TIME OF %s' % str(time.mktime(time.localtime())-time.mktime(start_time)))


    iq = mp.Queue()
    oq = mp.Queue()
    Stop = False
    while not Stop:
        Stop = True
        counter=0
        iq, counter = populate_queue(free_vars,iq,counter) #only free variables are populated
        print(counter)
        myprocesses = [mp.Process(target=varProb,args=(iq,oq)) for _ in range(pool_size)]
        for p in myprocesses:
            p.start()

        #manage output queue
        count = 0
        while count < counter: 
            try: 
                result = oq.get()
                if result[0]==1:
                    count+=1
                    my_message = result[1]
                    if 'infeasible' in my_message:
                        m=tuple(my_message.split()[0][3:].split('_')) #format string back to freevar format
                        var_status[m] = (0,0)
                        free_vars[m].lb=0
                        free_vars[m].ub=0
                        Stop=False
                        NFLmodel.update()
                    print( my_message + ' Queue: ' + str(count) + '/' + str(counter) )
                elif result[0] == 0:
                    my_message = result[1]
                    print( Back.GREEN + Fore.BLACK + my_message + ' Queue: ' + str(count) + '/' + str(counter) )
                else:
                    print(result)
            except:
                time.sleep(.5)
        NFLmodel.write('updated.lp')
    
    killswitch(pool_size,iq)
# stop the routine from moving forward until all processes have completed
    # their assigned task.  Without this, you will get an error
    for p in myprocesses:
        p.join()

    # now that all processes are completed, terminate them all - you don't want
    # to tie up the CPU with zombie processes
    for p in myprocesses:
        p.terminate()

    number_tasks = oq.qsize()
    for i in range(number_tasks):
        print(oq.get_nowait()[1])

    # There may be some left over "Nones" in the input queue.  Let's clear 
    # them out since we want to account for all tasks (good housekeeping)
    number_tasks = iq.qsize()
    for i in range(number_tasks):
        try:
            iq.get_nowait()
        except:
            pass

    print('(MASTER): COMPLETED FLUSHING QUEUE WITH A TOTAL RUN TIME OF %s' % str(time.mktime(time.localtime())-time.mktime(start_time)))
    return free_vars, var_status
def main():

    #load constraints
    set_constrained_variables()
    free_vars, var_status = get_variables()
    free_vars, var_status = MyHandler(free_vars,pool_size, var_status, start_time, NFLmodel) #do probing and all that parallel
    write = pd.DataFrame.from_dict(var_status,orient="index") #write solution
    write.to_csv("GameBounds.csv")
    NFLmodel.write('updated.lp')
    print('(MASTER):  ALL PROCESSES HAVE COMPLETED WITH A TOTAL RUN TIME OF %s' % str(time.mktime(time.localtime())-time.mktime(start_time)))

if __name__ == "__main__":
    start_time = time.localtime()
    #set worker information
    pool_size = 4
    my_seed = 11111124
    random.seed(my_seed)
    #load model
    NFLmodel=grb.read('OR604 Model File v2.lp')
    main()