from multiprocessing.managers import BaseManager
import multiprocessing as mp 
import traceback
import random 
import time 
import sys
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
def linkToQueue(servant_number, ip_address):
    while True:
        try:
            # connect to the base manager
            m = BaseManager(address=(ip_address, 60000), authkey=b'TestNBMDistributed')
            m.register('input_queue')
            m.register('output_queue')
            m.connect()
            # define and instantiate the queues
            input_queue = m.input_queue()
            output_queue = m.output_queue()
            print("(SERVANT %s): CONNECTED TO THE QUEUE" % servant_number)
            break
        except:
            print('(SERVANT %s): NO QUEUE MANAGER FOUND; WAITING'% servant_number)
            time.sleep(1)
    return input_queue, output_queue


def varProb(iq,oq, servant_number):
    '''
    Script run on each worker node
    '''  
    # This is just a nice thing to track which process accomplished the work
    my_name = mp.current_process().name
    loopcounter = 0 #loop prep
    local_start_time = time.localtime()
    while True:
        try:
            task = iq.get()
            try:
                if task[0] == None:
                    break
                if task[2] != loopcounter:
                    NFLR = grb.read('temp.lp')
                    NFLR.setParam('OutputFlag', False )
                    NFLR.setParam('TimeLimit',10)
                    NFLR.setParam('Threads',1)
                    loopcounter+=1
                local_start_time = time.localtime()
                ##operate on the variable
                myVar = NFLR.getVarByName(task[1])
                myVar.lb = 1
                NFLR.update()
                NFLR.optimize()
                if NFLR.Status == grb.GRB.INFEASIBLE:
                    myVar.lb = 0
                    myVar.ub = 0
                    NFLR.update()
                    mymessage =   "(SERVER-%s-%s) " % (servant_number,my_name.upper()) + str(task[1]) + ' -- is infeasible Time: ' + str(time.mktime(time.localtime())-time.mktime(local_start_time))
                else:
                    myVar.lb = 0
                    NFLR.update()
                    mymessage =  Fore.BLACK + Back.GREEN + "(SERVER-%s-%s) " % (servant_number,my_name.upper()) + str(task[1]) + ' -- is good. Time: '  + str(time.mktime(time.localtime())-time.mktime(local_start_time))
                NFLR.update()
                oq.put((1,mymessage))
            except:
                oq.put((2,traceback.format_exc()))
        except:
            time.sleep(4)
    output_queue.put((0,"(SERVER %s - %s) FINISHED PROCESSING AND IS READY TO TERMINATE WITH A TOTAL RUN TIME OF %s" % (my_server, my_name ) + str(time.mktime(time.localtime())-time.mktime(local_start_time))))
    return

def servant_main():
    # set the parameters that we want to be able to change
    pool_size = 4                   # the number of parallel proceses per server
    servant_number = sys.argv[1]    # this is an argument passed in at run time
                                    # it is just a unique identifier that distinguishes
                                    # the server id at run time.  Should be a letter
                                    # or a number
    print("(SERVER %s): STARTED THE SERVANT PROCESS" % servant_number)

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
        print("(SERVER %s): COULD NOT FIND IP ADDRESS FILE FOR QUEUE - ABORTING PROCESS" % servant_number)
        return

    # establish connections to the input and output queues 
    input_queue, output_queue = linkToQueue(servant_number, ip_address)

    # create the parallel processes 
    processes = [mp.Process(target=varProb, args = (input_queue, output_queue, servant_number)) for x in range(pool_size)]

    # start the parallel processes
    for p in processes:
        p.start()

    # stop the routine from moving forward until all processes have completed
    # their assigned task.  Without this, you will get an error     
    for p in processes:
        p.join()

    # now that all processes are completed, terminate them all - you don't want
    # to tie up the CPU with zombie processes
    for p in processes:
        p.terminate()
    
    print("SERVER %s HAS TERMINATED ITS %s PROCESSES AND IS SHUTTING DOWN" % (servant_number, pool_size))

if __name__ == '__main__':
    colorama.init(autoreset=True)
    servant_main()
		
