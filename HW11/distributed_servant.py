from multiprocessing.managers import BaseManager
import multiprocessing as mp 
import time 
import traceback
import socket
import sys

# We are using a distributed queue.  We need to connect to it differently
# This routine connects to the queues (input and output)
def linkToQueue(servant_number, ip_address):
    while True:
        try:
            # connect to the base manager
            print(ip_address)
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

# This routine is actually the routine that is parallelized across all distributed
# processes.  All parallel processes do nothing but run this routine.  Everything 
# else is handled by the handler and the main.  
def myWorker(input_queue, output_queue, my_server):

    # This is just a nice thing to track which process accomplished the work
    my_name = mp.current_process().name

    # loop until told to break.
    while True:
        try:
            # get the next element in the queue
            task = input_queue.get()
            try:
                # if the first element in the queue task is None, it means
                # we are done and the process can terminate
                if task[0] is None:
                    break 

                # the task to be accomplished is the second element in the queue
                # perform the work assigned to the parallel process - in this case
                # the task is to sleep for a specified number of seconds.
                time.sleep(task[1])

                # output a message that the task has been completed
                my_message = "(SERVER %s - %s): PROCESSED LIST ELEMENT %s BY SLEEPING FOR %s SECONDS WITH A TOTAL RUN TIME OF " % (my_server, my_name.upper(), task[0], task[1])
                output_queue.put((1,my_message))
            except:
                # there was an error performing the task. Output a message indicating
                # there was an error and what the error was.
                output_queue.put((2,traceback.format_exc()))
        except:
            # there is no task currently in the queue.  Wait and check again
            time.sleep(1)

    # the tasks have all been cleared by the queue, and the processes has been
    # instructed to terminate.  Send a message indicating it is terminating/exiting
    output_queue.put((0,"(SERVER %s - %s) FINISHED PROCESSING AND IS READY TO TERMINATE WITH A TOTAL RUN TIME OF " % (my_server, my_name)))

    return


def servant_main():
    # set the parameters that we want to be able to change
    pool_size = 16                   # the number of parallel proceses per server
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
    processes = [mp.Process(target=myWorker, args = (input_queue, output_queue, servant_number)) for x in range(pool_size)]

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
    servant_main()
