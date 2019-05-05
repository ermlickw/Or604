import gurobipy as grb
import time
import pandas as pd
import multiprocessing as mp 
import traceback
import random
import time
import colorama
from colorama import Fore, Back, Style
from termcolor import colored
colorama.init(autoreset=True)

# We are using a distributed queue.  We need to connect to it differently
# This routine connects to the queues (input and output)
def linkToQueue(my_server):
    while True:
        try:
            # connect to the base manager
            m = BaseManager(address=(my_server, 60000), authkey=b'TestNBMDistributed')
            m.register('input_queue')
            m.register('output_queue')
            m.connect()

            # define and instantiate the queues
            input_queue = m.input_queue()
            output_queue = m.output_queue()
            print("(MASTER): CONNECTED TO THE QUEUE")
            break
        except:
            print('(MASTER): NO QUEUE MANAGER FOUND; WAITING')
            time.sleep(1)

    return input_queue, output_queue

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

def kill_switch(flood_size,iq):
    for i in range(flood_size*10):
        iq.put((None,None))

    print('(MASTER): COMPLETED LOADING QUEUE WITH NONES WITH A TOTAL RUN TIME OF %s' % str(time.mktime(time.localtime())-time.mktime(start_time)))

def clean_queue(iq,oq):
    # the way we have written the handler, it is possible there are still 
    # messages in the output queue that need to be delivered (the parallel process
    # received the sentinal, terminated, send a message and all of this happened
    # after we processed the correct number of outputs).  So lets make sure the
    # output queue is empty.  If not, then output the messages
    number_tasks = oq.qsize() + 1
    for i in range(number_tasks):
        try:
            result = oq.get_nowait()
            my_message = result[1]
            running_time = time.mktime(time.localtime())-time.mktime(start_time)
            my_message = result[1] + str(running_time)            
            print(my_message)
        except:
            pass

    # let the main sleep for 5 seconds so the distributed processes have a chance
    # to process the sentinals/poison pills.  If we don't do this, its possible
    # the master could clear the queue while there are still open processes.  
    # This would prevent the processes from terminating
    time.sleep(5)

    # There may be some left over "Nones" in the input queue.  Let's clear 
    # the out since we want to account for all tasks (good housekeeping)
    num_tasks = iq.qsize()
    for i in range(num_tasks): 
        try:
            iq.get_nowait()
        except:
            pass

def MyHandler(free_vars, var_status, iq, oq, start_time, NFLmodel, Stop, outercounter):
    '''
    Main handler process to manage input queue and output queue 
    '''
    def populate_queue(freevars,inputqueue, varsleft, outercounter):
        for v in freevars:
            if freevars[v].lb != freevars[v].ub:
                varname = 'GO_' + '_'.join(list(v))
                inputqueue.put((outercounter,varname)) # dont have to pickle just needs to be pickeable - sending outerloop count and varaible to evaluate
                varsleft+=1
        print('(MASTER): COMPLETED LOADING QUEUE WITH TASKS WITH A TOTAL RUN TIME OF %s' % str(time.mktime(time.localtime())-time.mktime(start_time)))
    return inputqueue, varsleft

    #manage input and output queue
    varsleft = 0
    iq, varsleft = populate_queue(free_vars,iq,varsleft, outercounter) #send variables to input queue to evaluate
    print(varsleft)
    Stop = True #loop prep
    innercount = 0
        while innercount < varsleft: 
            try: 
                result = oq.get()
                if result[0]==1:
                    innercount+=1
                    my_message = result[1]
                    if 'infeasible' in my_message:
                        m=tuple(my_message.split()[0][3:].split('_')) #format string back to freevar format
                        var_status[m] = (0,0)
                        free_vars[m].lb=0
                        free_vars[m].ub=0
                        Stop=False
                        NFLmodel.update()
                    print( my_message + ' Queue: ' + str(innercount) + '/' + str(varsleft) )
                elif result[0] == 0:
                    my_message = result[1]
                    print(  my_message + ' Queue: ' + str(innercount) + '/' + str(varsleft))
                else:
                    print(result)
            except:
                time.sleep(.5)
            
        NFLmodel.write('updated.lp')
    

    return free_vars, var_status, Stop


def server_main(flood_size):
    '''
    Main function
    '''
    # connect to the distributed queue
    iq, oq = linkToQueue(ip_address)
    #model initial pruning
    NFLmodel=grb.read('OR604 Model File v2.lp')
    set_constrained_variables()
    free_vars, var_status = get_variables()
    NFLmodel.write('temp.lp') #write prelim reduced results
    Stop = False #loop prep
    outercounter = 1 #for counting how many outer loops
    while not Stop:
        free_vars, var_status, Stop = MyHandler(free_vars, var_status, iq, oq, start_time, NFLmodel, Stop, outercounter) #do var probing round
        outercounter +=1
        write = pd.DataFrame.from_dict(var_status,orient="index") #write solution
        write.to_csv("GameBoundsTemp.csv")
        NFLmodel.write('temp.lp')
    kill_switch(flood_size,iq)
    clean_queue(iq,oq)
    #write final output
    NFLmodel.write('Final.lp')
    write = pd.DataFrame.from_dict(var_status,orient="index") #write solution
    write.to_csv("GameBoundsFinal.csv")
    print('(MASTER):  ALL PROCESSES HAVE COMPLETED WITH A TOTAL RUN TIME OF %s' % str(time.mktime(time.localtime())-time.mktime(start_time)))



if __name__ == "__main__":
    start_time = time.localtime()
    # set the parameters that we want to be able to change
    my_seed = 11111124      # the seed number we use in all three experiments 
    random.seed(my_seed)    # fix the seed value for the random number generator
    servants = 2
    nodes_per_servant = 4
    flood_size = servants * nodes_per_servant * 10
    print("(MASTER): STARTED THE MASTER PROCESS")
    # get the IP address that is hosting the queue.  Because we are running
    # our processes on several servers, the server the queue is running on
    # may not be the server the master process is running on.  The queue manager
    # routine identifies the server IP address and writes the file to a specified
    # location
    try:
        ip_file = ".\\queue_ip.txt"
        with open(ip_file, 'r') as my_file:
            ip_address = my_file.readline().strip()
    except:
        print("(MASTER): COULD NOT FIND IP ADDRESS FILE FOR QUEUE - ABORTING PROCESS")
        exit()
        return
    
    #start working on prelim tasks
    server_main(flood_size)
