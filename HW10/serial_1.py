import random
import time

# This routine does the work that needs to be completed for each task.  
def myWorker(i,sleep_time):
    # do the work required by the task - in this case it is to sleep for a specified
    # number of seconds
    time.sleep(sleep_time)
    my_message = "(MASTER): PROCESSED LIST ELEMENT %s BY SLEEPING FOR %s SECONDS WITH A TOTAL RUN TIME OF " % (i,sleep_time)
    return my_message

# This routine manages all of the tasks that need to be worked
def myHandler(sleeper_list, start_time):

    # manage the processing of all tasks and output the results of work accomplished
    for i in range(len(sleeper_list)):
        my_message = myWorker(i,sleeper_list[i])
        running_time = time.mktime(time.localtime())-time.mktime(start_time)
        my_message = my_message + str(running_time)
        print(my_message)
    return

def main():
    # set the start time
    start_time = time.localtime()
    
    # set the parameters that we want to be able to change
    list_size = 25         # the size of the list that holds the sleep time
    my_seed = 11111124      # the seed number we use in all three experiments
                            # so that we know we are processing the same tasks

    random.seed(my_seed)    # fix the seed value for the random number generator

    # instantiate and populate the list of sleep times (for real problems these
    # would be the individual tasks we are trying to complete)
    sleeper_list = []
    for i in range(list_size):
        sleeper_list.append(random.randint(1,4))

    # call the routine that manages the tasks 
    myHandler(sleeper_list, start_time)

    # notify that all tasks are done
    print('(MASTER):  ALL TASKS HAVE COMPLETED WITH A TOTAL RUN TIME OF %s' % (time.mktime(time.localtime())-time.mktime(start_time)))

    return

if __name__ == "__main__":
    main()