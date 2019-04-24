import multiprocessing as mp 
import traceback
import random
import time

def varProb(iq,oq):
    NFLR = grb.read('../models/temp.lp')
    NFLR.setParam('logtoconsole',0)
    NFLR.setParam('timelimit',10)
    NFLR.setParam('threadlimit',1)
    while True:
        try:
            task = iq.get()
            try:
                if task = None:
                    break
                ##operate on the variable
                myVar = NFLR.getVarbyName(task)
                myVar.lb = 1
                NFLR.update()
                NFLR.optimize()
                if NFLR.Status == grb.GRB.INFESIBLE:
                    myVar.ub = 0
                    mymessage = task + ': is infeasible'
                else:
                    myVar.lb = 0
                    mymessage = task + ': is good'
                NFLR.update()
                oq.put(1,mymessage)
        except:
            time.sleep(4)

def MyHandler(free_vars,pool_size):

    def populate_queue(freevars,inputqueue):
        for v in freevars:
            if freevars[v][0] != freevars[v][1]:
                inputqueue.put(v) # dont have to pickle just needs to be pickeable
                counter+=1
        return counter

    iq = mp.Queue()
    oq = mp.Queue()
    while not STOP:
        populate_queue(free_vars,iq) #only free variables are populated
        myprocesses = [mp.process(target=varProb,args=(iq,oq) for _ in range(pool_size))]
        for p in myprocesses:
            p.start()
        #manage output queue

        #if result[2]=='infesible' stop = FALSE

        #join 
        #terminate
        #final checks

def main():
    pool_size = 4
    GETVARS()
    FindZeros()
    MYHandler()
