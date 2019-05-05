from multiprocessing.managers import BaseManager
import multiprocessing as mp 
import traceback
import random 
import time 

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

# This routine preps, manages, and terminates the parallel processes 
def myHandler(ip_address, sleeper_list, flush_size, start_time):

    # connect to the distributed queue
    input_queue, output_queue = linkToQueue(ip_address)

    # load the queue with the tasks to be accomplished
    list_size = len(sleeper_list)
    for i in range(list_size):
        input_queue.put((i,sleeper_list[i]))
    print('(MASTER): COMPLETED LOADING QUEUE WITH TASKS WITH A TOTAL RUN TIME OF %s' % (time.mktime(time.localtime())-time.mktime(start_time)))

    # load the queue with sentinals/poison pills that terminate the parallel processes
    for i in range(flush_size): 
        input_queue.put((None,None))
    print('(MASTER): COMPLETED LOADING QUEUE WITH NONES WITH A TOTAL RUN TIME OF %s' % (time.mktime(time.localtime())-time.mktime(start_time)))

    # manage the results provided by each of the distributed processes
    counter = 0     # this variable is used to count the number of solutions we
                    # are looking for so that we know when we are done
    while counter < list_size:
        try:
            result = output_queue.get()
            try:
                # if result = 1, then we know this is a "countable" output
                if result[0] == 1:
                    counter += 1
                    running_time = time.mktime(time.localtime())-time.mktime(start_time)
                    my_message = result[1] + str(running_time)
                    print(my_message)
                    # if result = 0, then we know this is information that is useful, but
                    # not a countable output
                elif result[0] == 0:
                    my_message = result[1]
                    running_time = time.mktime(time.localtime())-time.mktime(start_time)
                    my_message = result[1] + str(running_time)
                    print(my_message)
                # any other type of result indicates we had an error and we want to 
                # see what the error was so we can fix it.  This is what makes 
                # parallel processing so hard
                else:
                    print(result + '\n' + traceback.format_exc())
            except:
                # There was an error in processing the queue result, output 
                # the error message
                print(traceback.format_exc())                
        except:
            # there was nothing in the queue (yet) wait a little and check again
            time.sleep(.5)

    # the way we have written the handler, it is possible there are still 
    # messages in the output queue that need to be delivered (the parallel process
    # received the sentinal, terminated, send a message and all of this happened
    # after we processed the correct number of outputs).  So lets make sure the
    # output queue is empty.  If not, then output the messages
    number_tasks = output_queue.qsize() + 1
    for i in range(number_tasks):
        try:
            result = output_queue.get_nowait()
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
    num_tasks = input_queue.qsize()
    for i in range(num_tasks): 
        try:
            input_queue.get_nowait()
        except:
            pass

    # close out the handler process
    print('(MASTER): COMPLETED FLUSHING QUEUE WITH A TOTAL RUN TIME OF %s' % (time.mktime(time.localtime())-time.mktime(start_time)))

    return 

def master_main():
    # set the start time
    start_time = time.localtime()

    # set the parameters that we want to be able to change
    list_size = 10000         # the size of the list that holds the sleep time
    my_seed = 11111124      # the seed number we use in all three experiments
                            # so that we know we are processing the same tasks    
    flush_size = 100         # the number of "Nones" to put in the queue to flush
    print("(MASTER): STARTED THE MASTER PROCESS")

    random.seed(my_seed)    # fix the seed value for the random number generator

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
        return

    # instantiate and populate the list of sleep times (for real problems these
    # would be the individual tasks we are trying to complete)
    sleeper_list = []
    for i in range(list_size):
        sleeper_list.append(random.randint(1,4))

    # call the handler that does most of the coordination of parallel process
    # for the main.
    myHandler(ip_address, sleeper_list, flush_size, start_time)
    
    # notify that the process is done
    print('(MASTER): TOTAL RUNNING TIME:= %s' % str(time.mktime(time.localtime()) - time.mktime(start_time)))    
    return

if __name__ == "__main__":
    master_main()