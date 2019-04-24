import multiprocessing as mp 
import traceback
import random
import time


# This routine is actually the routine that is parallelized.  All parallel 
# processes do nothing but run this routine.  Everything else is handled by
# the handler and the main.  
def myWorker(input_queue, output_queue, start_time):

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
                my_message = "(%s): PROCESSED LIST ELEMENT %s BY SLEEPING FOR %s SECONDS WITH A TOTAL RUN TIME OF " % (my_name.upper(), task[0], task[1])
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
    output_queue.put((0,"(%s) FINISHED PROCESSING AND IS READY TO TERMINATE WITH A TOTAL RUN TIME OF  %s SECONDS" % (my_name.upper(),(time.mktime(time.localtime())-time.mktime(start_time)))))
    return

# This routine preps, manages, and terminates the parallel processes 
def myHandler(sleeper_list, pool_size, start_time):

    # establish connections to the input and output queues 
    input_queue = mp.Queue()
    output_queue = mp.Queue()

    # load the queue with the tasks to be accomplished
    for i in range(len(sleeper_list)):
        input_queue.put((i,sleeper_list[i]))
    print('(MASTER): COMPLETED LOADING QUEUE WITH TASKS WITH A TOTAL RUN TIME OF %s' % (time.mktime(time.localtime())-time.mktime(start_time)))

    # load the queue with sentinals/poison pills that terminate the parallel processes
    for i  in range(pool_size*2):
        input_queue.put((None,None))
    print('(MASTER): COMPLETED LOADING QUEUE WITH NONES WITH A TOTAL RUN TIME OF %s' % (time.mktime(time.localtime())-time.mktime(start_time)))

    # create the parallel processes 
    my_processes = [mp.Process(target=myWorker,args=(input_queue,output_queue, start_time)) for _ in range(pool_size)]

    # start the parallel processes
    for p in my_processes:
        p.start()

    # manage the results provided by each of the parallel processes
    counter = 0     # this variable is used to count the number of solutions we
                    # are looking for so that we know when we are done
    while counter < len(sleeper_list):
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
                    print(my_message)
                # any other type of result indicates we had an error and we want to 
                # see what the error was so we can fix it.  This is what makes 
                # parallel processing so hard
                else:
                    print(result)
            except:
                # There was an error in processing the queue result, output 
                # the error message
                print(traceback.format_exc())
        except:
            # there was nothing in the queue (yet) wait a little and check again
            time.sleep(.5)

    # stop the routine from moving forward until all processes have completed
    # their assigned task.  Without this, you will get an error
    for p in my_processes:
        p.join()

    # now that all processes are completed, terminate them all - you don't want
    # to tie up the CPU with zombie processes
    for p in my_processes:
        p.terminate()

    # the way we have written the handler, it is possible there are still 
    # messages in the output queue that need to be delivered (the parallel process
    # received the sentinal, terminated, send a message and all of this happened
    # after we processed the correct number of outputs).  So lets make sure the
    # output queue is empty.  If not, then output the messages
    number_tasks = output_queue.qsize()
    for i in range(number_tasks):
        print(output_queue.get_nowait()[1])

    # There may be some left over "Nones" in the input queue.  Let's clear 
    # them out since we want to account for all tasks (good housekeeping)
    number_tasks = input_queue.qsize()
    for i in range(number_tasks):
        try:
            input_queue.get_nowait()
        except:
            pass

    # close out the handler process
    print('(MASTER): COMPLETED FLUSHING QUEUE WITH A TOTAL RUN TIME OF %s' % (time.mktime(time.localtime())-time.mktime(start_time)))


    return

def main():
    # set the start time
    start_time = time.localtime()

    # set the parameters that we want to be able to change
    list_size = 25         # the size of the list that holds the sleep time
    pool_size = 4           # the number of parallel processes we want to run
    my_seed = 11111124      # the seed number we use in all three experiments
                            # so that we know we are processing the same tasks

    random.seed(my_seed)    # fix the seed value for the random number generator

    # instantiate and populate the list of sleep times (for real problems these
    # would be the individual tasks we are trying to complete)
    sleeper_list = []
    for i in range(list_size):
        sleeper_list.append(random.randint(1,4))

    # call the routine that manages the pool of processes 
    myHandler(sleeper_list, pool_size, start_time)

    # notify that the process is done
    print('(MASTER):  ALL PROCESSES HAVE COMPLETED WITH A TOTAL RUN TIME OF %s' % (time.mktime(time.localtime())-time.mktime(start_time)))

    return

if __name__ == "__main__":
    main()