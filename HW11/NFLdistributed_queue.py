from multiprocessing.managers import BaseManager
from queue import Queue
import socket
import os

# instantiate the queues
input_queue = Queue()
output_queue = Queue()

# connect to the base manager
manager = BaseManager(address=('',60000), authkey = b'TestNBMDistributed')
manager.register('input_queue', callable = lambda:input_queue)
manager.register('output_queue', callable = lambda:output_queue)
print('queues started')

# the queue will only be started on one server and not all processes will be
# run on the server.  So we write down the IP address of the server on which
# the queues are running and being managed and then have the other servers
# read the file.  
# get the current directory in whcih the queue code is being run
cur_dir = os.path.dirname(os.path.abspath(__file__))

# open a txt file and write the ip address in the file
with open(cur_dir + '\\queue_ip.txt', 'w') as my_file:
    q_ip_address = str(socket.gethostbyname(socket.gethostname()))
    my_file.write(q_ip_address)
print('queue IP address written to file')

# have the queue run until it is manually terminated
server = manager.get_server()
server.serve_forever()


