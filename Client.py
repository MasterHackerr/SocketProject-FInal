'''
Daniel Quan
1216497750

CSE 434
Syrotiuk

Group: 100

Ports: 43000 - 43499

Client.py:

    This file contains the Client class and sub-classes and manages the interaction
    between the client nodes.

'''

from Server import UDPServer
from csv import DictReader

from _thread import *

import json
import sys
import os


# Each Client has its own instance when running (A new Client instance will be instantiated for each client)
class Client:

    def __init__(self, username, serv_ip, serv_port, client_ip, client_port, query_port, right_port, hash_size,
                 buff_size, file_path):

        # constants
        self.BUFFER_SIZE = buff_size  # Set the buffer size of the client
        self.HASH_SIZE = hash_size  # set the hash size of the client
        self.FILE_PATH = file_path  # set the file path of the client

        # Create a subclass called user
        self.user = self.User(username, (serv_ip, serv_port), (client_ip, client_port), (client_ip, query_port),
                              (client_ip, right_port))

        # Client_Server subclass
        self.sockets = self.Client_Server()

        # Local hash table stored inside the client instance
        self.local_hash_table = [[] for _ in range(hash_size)]

        # Booleans and checks for each client to keep track of

        self.record = None  # record
        self.query = None  # query
        self.started_a_query = False  # Bool to indicate if a new query was started
        self.new_leader = None  # The new indicated leader
        self.leaving_user = False  # Leaving User
        self.joining_user = False  # Joining User
        self.check_started = False  # Check Started

    # Client_Server Class
    class Client_Server:

        # UDP Server Sockets
        # Instantiate Client_Server instance
        def __init__(self):
            self.client_to_server = UDPServer()
            self.accept_port = UDPServer()
            self.query_port = UDPServer()
            self.send_port = UDPServer()

    # User class
    class User:

        # Client user information
        # Instantiate a user instance
        def __init__(self, username, server_addr, accept_addr, query_addr, send_addr):
            self.username = username
            self.server_addr = server_addr
            self.accept_port_address = accept_addr
            self.query_addr = query_addr
            self.send_port_addr = send_addr
            self.next_node_addr = None
            self.next_node_query_addr = None
            self.prev_node_addr = None
            self.id = None
            self.n = None
            self.dht = None

    # start_threads function
    # Begin threads of client servers to read in received packages
    def start_threads(self):

        # Start client server
        print('Starting client topology socket\n')
        start_new_thread(self.initialize_acceptance_port, ())

        # Start client query server
        print("Starting client query socket\n")
        start_new_thread(self.client_query_socket, ())

    # Function to set userdata after receiving info from the DHT leader
    def set_data(self, data, index=0):

        self.user.dht = data
        self.user.prev_node_addr = (data[index]['ip'], int(data[index]['port']))
        self.user.id = data[index + 1]['id']
        self.user.n = data[index + 1]['n']
        self.user.next_node_addr = (data[index + 2]['ip'], int(data[index + 2]['port']))
        self.user.next_node_query_addr = (data[index + 2]['ip'], int(data[index + 2]['query']))

    # Function that is used to debug when outputting node info
    def num_of_records(self):

        count = 0
        for list_of_records in self.local_hash_table:
            for _ in list_of_records:
                count += 1

        return f"\tRecords in hash: {count}"

    # Prints info on the user instance (for debugging)
    def output_node_info(self):

        print(json.dumps(vars(self.user), sort_keys=False, indent=4))
        print("\n", self.num_of_records())

    # Calculate pos variable with hash function
    def hash_pos(self, record):

        ascii_sum = 0
        for letter in record['Long Name']:
            ascii_sum += ord(letter)

        return ascii_sum % self.HASH_SIZE

    # Terminates the end script
    def end_script(self, message):

        if message:
            print(message)
        sys.exit()

    # Function to check the given record and see if it's stored on the local hash table.
    # If it's not set, the self.record value to which triggers the query socket and  sends the query command to the next
    def check_record(self, record):

        pos = self.hash_pos(record)
        id = pos % self.user.n

        if id == self.user.id:
            # If it's the desired location for the record
            self.local_hash_table[pos].append(record)
        else:

            # try to send response to the designated next node address
            try:
                self.sockets.send_port.send_response(addr=self.user.next_node_addr, res='SUCCESS', type='record',
                                                     data=record)

            # if the response can't be sent to the next node
            except:
                print("client-node: sendall() error within records connect nodes")

    # setup all local dht functions
    # Function reads in records one at a time and calls to check the record
    def setup_all_local_dht(self, print_input=True):

        with open(os.path.join(sys.path[0], self.FILE_PATH), "r") as data_file:
            csv_reader = DictReader(data_file)
            total_records = 0

            # Iterate over each row in the csv file using the reader object
            print("\nSending records through DHT to store.\n")
            for record in csv_reader:

                self.check_record(record)
                total_records += 1
                if total_records % 50 == 0:
                    print(f"\t{total_records} records stored so far...")

            print(f"\n\t{total_records} records stored in total")
            if print_input:
                print("\nEnter command for the server: ")

    # dht teardown function
    # teardown of dht by removing all information on the user interface. Resets the hash table to empty as well.
    def teardown_dht(self, leaving):

        # Teardown the local DHT (only), ID's and neighbors are unaffected
        self.local_hash_table = [[] for _ in range(self.HASH_SIZE)]

        # If the user is not leaving, set all fields to None (Null)
        if not leaving:
            self.user.id = None
            self.user.n = None
            self.user.dht = None
            self.user.next_node_addr = None
            self.user.next_node_query_addr = None
            self.user.prev_node_addr = None

    def initialize_acceptance_port(self):

        # Attempt to bind the sockets and establish the connection
        try:
            self.sockets.accept_port.socket.bind(self.user.accept_port_address)

        # Otherwise indicate that the connection wasn't established
        except Exception as error:
            print(error)
            print(f"server: bind() failed for client: {self.user.accept_port_address}")
            return

        # Print what port the server is listening to
        print(f"client-server: Port server is listening to is: {self.user.accept_port_address[1]}\n")

        # Add loop here so that we can disconnect and reconnect to server
        while True:
            message, addr = self.sockets.accept_port.socket.recvfrom(self.BUFFER_SIZE)

            # print(f"Client-server received message from addr: {addr}")

            self.client_acceptance(message, addr)

    # Client acceptance function
    # Maintains a connection with the neighboring client until the neighbor initiates the disconnect
    def client_acceptance(self, data, addr):

        if data:

            data_loaded = data.decode('utf-8')
            data_loaded = json.loads(data_loaded)

            # print(f"client-topology: received message ``{data_loaded}''\n")

            if data_loaded['type'] == 'record':
                self.check_record(record=data_loaded['data'])
            elif data_loaded['type'] == 'set-id':

                self.set_data(data_loaded['data'])
                # print(vars(self.user))

                self.sockets.accept_port.socket.sendto(b'SUCCESS', addr)

            elif data_loaded['type'] == 'leaving-teardown':

                # Call teardown but leave the var set to True
                self.teardown_dht(True)

                # if the current user is leaving
                if self.leaving_user:
                    print('Teardown complete now calling reset-id\n')
                    # Call to reset every node id
                    self.sockets.send_port.send_response(addr=self.user.next_node_addr, res='SUCCESS', type='reset-id', data=0)

                # If the current user is joining
                elif self.joining_user:
                    print("Teardown complete now rebuilding the DHT\n")
                    # The next node is the leader, call for rebuild of DHT
                    self.sockets.send_port.send_response(addr=self.user.next_node_addr, res='SUCCESS', type='rebuild-dht', data=self.user.accept_port_address)

                else:
                    # Continue teardown
                    self.sockets.send_port.send_response(addr=self.user.next_node_addr, res='SUCCESS', type='leaving-teardown')

            elif data_loaded['type'] == 'teardown':

                # If the user ID is 0
                if self.user.id == 0:

                    # Don't teardown dht
                    self.teardown_dht(False)

                    # Send successful command to server
                    self.sockets.client_to_server.socket.sendto(
                        bytes(f'teardown-complete {self.user.username}', 'utf-8'), self.user.server_addr)
                    # Listen on the socket
                    self.listen()

                else:

                    # Set the next node
                    next_node_addr = self.user.next_node_addr

                    # Don't initiate teardown
                    self.teardown_dht(False)

                    # Send success message
                    self.sockets.send_port.send_response(addr=next_node_addr, res='SUCCESS', type='teardown')

            elif data_loaded['type'] == 'reset-id':
                new_id = int(data_loaded['data'])

                # If the user is not leaving
                if not self.leaving_user:

                    self.user.id = new_id
                    self.user.n = self.user.n - 1
                    new_id += 1
                    self.sockets.send_port.send_response(addr=self.user.next_node_addr, res='SUCCESS', type='reset-id',
                                                         data=new_id)
                else:
                    # Nodes have been renumbered
                    print("Node ID's successfully changed")

                    # Call to convert neighbors or nodes
                    self.convert_neighbors()

            elif data_loaded['type'] == 'reset-n':

                if self.user.id == 0:

                    # The current is the leader so set previous node and the new n
                    self.user.n = self.user.n + 1
                    data_loaded['data']['n'] = self.user.n
                    self.user.prev_node_addr = data_loaded['data']['addr']
                    self.sockets.send_port.send_response(addr=self.user.next_node_addr, res='SUCCESS', type='reset-n', data=data_loaded['data'])

                # If the username doesn't match the loaded username
                elif self.user.username != data_loaded['data']['username']:

                    self.user.n = self.user.n + 1

                    if self.user.n - 2 == self.user.id:
                        data_loaded['data']['prev'] = self.user.accept_port_address
                        self.user.next_node_addr = tuple(data_loaded['data']['addr'])
                        self.user.next_node_query_addr = tuple(data_loaded['data']['query'])
                    self.sockets.send_port.send_response(addr=self.user.next_node_addr, res='SUCCESS', type='reset-n', data=data_loaded['data'])

                else:

                    # Nodes have successfully been renumbered
                    print("Node size successfully changed\n")
                    self.user.prev_node_addr = data_loaded['data']['prev']
                    self.user.n = data_loaded['data']['n']
                    self.user.id = self.user.n - 1

                    print("Teardown the existing DHT\n")

                    # Teardown the current DHT
                    self.sockets.send_port.send_response(addr=self.user.next_node_addr, res='SUCCESS',
                                                         type='leaving-teardown')

            # Reset left selected
            elif data_loaded['type'] == 'reset-left':

                # print(data_loaded['data'])
                # If the next user matches with the current, this is the previous node
                if self.user.next_node_addr == tuple(data_loaded['data']['current']):

                    # print("This is the prev node")        # For testing

                    self.user.next_node_addr = tuple(data_loaded['data']['new'])
                    self.user.next_node_query_addr = tuple(data_loaded['data']['query'])

                    # Send response that reset current was successful
                    self.sockets.send_port.send_response(addr=tuple(data_loaded['data']['current']), res='SUCCESS',
                                                         type='reset-complete')

                else:
                    # Send response that the reset left was successful
                    self.sockets.send_port.send_response(addr=self.user.next_node_addr, res='SUCCESS',
                                                         type='reset-left', data=data_loaded['data'])

            # Reset right selected
            elif data_loaded['type'] == 'reset-right':
                self.user.prev_node_addr = tuple(data_loaded['data'])
                self.sockets.send_port.socket.sendto(bytes(self.user.username, 'utf-8'), addr)

            # If the reset is determined to be complete
            elif data_loaded['type'] == 'reset-complete':
                print('Received reset complete\nNow rebuilding DHT')
                res_data = self.user.accept_port_address
                self.sockets.send_port.send_response(addr=self.user.next_node_addr, res='SUCCESS', type='rebuild-dht',
                                                     data=res_data)

            # If the rebuild DHT is selected
            elif data_loaded['type'] == 'rebuild-dht':

                print("Received rebuild DHT command\nSetting up node ring")

                # self.new_leader = self.username           # Set new leader. Rebuilding the DHT

                self.setup_all_local_dht()

                # Send success response about the DHT rebuild operation
                self.sockets.send_port.send_response(addr=tuple(data_loaded['data']), res='SUCCESS', type='dht-rebuilt')

            # If the DHT has been rebuilt
            elif data_loaded['type'] == 'dht-rebuilt':

                success_string = bytes(f'dht-rebuilt {self.user.username} {self.new_leader}', 'utf-8')

                # If the joining user is successful
                if self.joining_user:
                    self.joining_user = False
                    success_string = bytes(f'dht-rebuilt {self.user.username}', 'utf-8')
                self.leaving_user = False

                # Attempt to send a message and listen to socket
                try:
                    self.sockets.client_to_server.socket.sendto(success_string, self.user.server_addr)
                    self.listen()

                # Otherwise print an error
                except:
                    print("client: sendall() error sending success string")
                    return

            # If check nodes has been selected
            elif data_loaded['type'] == 'check-nodes':

                self.output_node_info()  # Print node info
                if not self.check_started:
                    self.check_started = False
                else:
                    self.sockets.send_port.send_response(addr=self.user.next_node_addr, res='SUCCESS', type='check-nodes')

    # client query socket function
    # Set up a socket for the query port which accepts all connections and stats a new listening thread
    def client_query_socket(self):

        # Attempt to bind the socket
        try:
            self.sockets.query_port.socket.bind(self.user.query_addr)

        # Otherwise print an error
        except Exception as error:
            print(error)
            print("query-server: bind() failed")
            return

        print(f"query-server: Port server is listening to is: {self.user.query_addr[1]}\n")

        # Loop here so that one can disconnect and reconnect to the server
        while True:
            message = self.sockets.query_port.socket.recv(self.BUFFER_SIZE)
            self.client_query_conn(message)

# client query connection
# Socket connection listener that listens for query commands
    def client_query_conn(self, data):

        if data:
            data_loaded = data.decode('utf-8')

            if data_loaded:
                # Set data load
                try:
                    data_loaded = json.loads(data_loaded)

                # Otherwise print error
                except:
                    self.end_script("error with json.load")

            #If the loaded data is not a string
            if type(data_loaded['data']) != str:

                # Set the record
                record = data_loaded['data']

                # If the record is None
                if record == None:
                    print(f"\n\nQuery for Long Name of {self.query}: 404 record not found\n")
                else:
                    print(f"\n\nQuery for Long Name of {self.query}:\n")
                    print(json.dumps(data_loaded['data'], sort_keys=False, indent=4))
                self.query = None
                return

            # The data is a string so we can use the split method to create a list
            data_list = data_loaded['data'].split()

            if data_list[0] == 'query':
                addr = tuple(data_loaded['origin'])
                self.run_query(addr, data_list[1:])
            else:
                print(json.dumps(data_loaded, sort_keys=False, indent=4))

# run query function
# Take the query command and return either a response with a found  record or a call to the next node with the same query command
    def run_query(self, addr, long_name):

        pos = self.hash_pos({'Long Name': ' '.join(long_name)})

        # set the id
        id = pos % self.user.n

        # if the ID equals the current user ID
        if id == self.user.id:

            # This is the correct node for query
            records = self.local_hash_table[pos]

            # Loop to find records
            for record in records:

                if record['Long Name'] == ' '.join(long_name):
                    self.started_a_query = False
                    self.sockets.query_port.send_response(addr, res='SUCCESS', type='query-result', data=record)
                    return

            self.started_a_query = False
            self.sockets.query_port.send_response(addr, res='FAILURE', type='query-result')

        # Otherwise
        else:
            # This isn't the correct node for query
            self.query = ' '.join(long_name)
            self.connect_query_nodes(addr)

# Connect query nodes function
# Connect to the query address given or to the address of the next node
    def connect_query_nodes(self, origin, ip=None, port=None):

        # Prevents the infinite looping through nodes
        if self.started_a_query and not ip:

            self.started_a_query = False
            self.sockets.query_port.send_response(origin, res='FAILURE', type='query-result')
            return

        if self.query:

            query_info = 'query ' + self.query
            data = {

                'data': query_info,
                'origin': origin

            }
            data_loaded = json.dumps(data)
            query = bytes(data_loaded, 'utf-8')

            try:
                if ip:

                    # print(f'sending query info {data_loaded} to address {ip}, {port}') # For testing
                    self.started_a_query = True
                    self.sockets.query_port.socket.sendto(query, (ip, port))

                else:

                    # print(f'sending query info {data_loaded} to address {self.user.next_node_query_addr}')    # For testing
                    self.sockets.query_port.socket.sendto(query, self.user.next_node_query_addr)

                # Query sent, now awaiting response from the next node
            except:
                print("client-node: sendall() error within query connection")
                return

        # Otherwise
        else:
            # Send the failure response
            failure_res = "missing query, put: 'query {Long Name}' in your query command"
            self.sockets.query_port.send_response(origin, res='FAILURE', type='query-result', data=failure_res)

# connect all nodes function
    def connect_all_nodes(self):

        i = 1

        # Loop the dht length
        while i < len(self.user.dht):

            j = i - 1

            if i + 1 < len(self.user.dht):
                data = (self.user.dht[j], self.user.dht[i], self.user.dht[i + 1])
            else:
                data = (self.user.dht[j], self.user.dht[i], self.user.dht[0])
            # print("Sending data: ", data)

            # Try sending response and buffer size
            try:
                print(f'sending message to addr: ({self.user.dht[i]["ip"]}, {self.user.dht[i]["port"]})')
                self.sockets.send_port.send_response(addr=(self.user.dht[i]['ip'], int(self.user.dht[i]['port'])),
                                                     res='SUCCESS', type='set-id', data=data)
                self.sockets.send_port.socket.recv(self.BUFFER_SIZE)
                print(f"Successfully sent set-id command to {self.user.dht[i]['username']}")

            # If node connection data was not successfully sent
            except Exception as error:
                print(error)
                print('An exception occurred sending node connection data')

            # Increment
            i += 1

# Convert neighbors function
    def convert_neighbors(self):

        reset_right_data = self.user.prev_node_addr

        # Send  reset right command and wait for a response
        self.sockets.send_port.send_response(addr=self.user.next_node_addr, res='SUCCESS', type='reset-right', data=reset_right_data)

        res = self.sockets.send_port.socket.recv(self.BUFFER_SIZE)

        # Check for success message from res
        data_loaded = res.decode('utf-8')

        # print(f'REset right sent back {data_loaded}')     # Testing function
        self.new_leader = data_loaded

        # reset left data
        reset_left_data = {

            'current': self.user.accept_port_address,
            'new': self.user.next_node_addr,
            'query': self.user.next_node_query_addr

        }

        # Send success response
        self.sockets.send_port.send_response(addr=self.user.next_node_addr, res='SUCCESS', type='reset-left', data=reset_left_data)

# check nodes function
    def check_nodes(self):

        # check started bool
        self.check_started = True

        # Send success response
        self.sockets.send_port.send_response(addr=self.user.next_node_addr, res='SUCCESS', type='check-nodes')

# listen function
    def listen(self):

        data = self.sockets.client_to_server.socket.recv(self.BUFFER_SIZE)
        data_loaded = data.decode('utf-8')

        if data_loaded:
            # Try to load data
            try:
                data_loaded = json.loads(data_loaded)

            # If the json load fails
            except:
                print("error with json.load")
                return

        print("\n\n")

        if data_loaded and data_loaded['res'] == 'SUCCESS':

            if data_loaded['data']:
                print(json.dumps(data_loaded, sort_keys=False, indent=4))

            if data_loaded['type'] == 'DHT':

                self.set_data(data_loaded['data'], index=-1)
                self.connect_all_nodes()

                # Call setup all dht and set the input printer variable to false
                self.setup_all_local_dht(False)

                success_string = bytes(f'dht-complete {self.user.username}', 'utf-8')

                # try to send a message through the socket and listen
                try:
                    self.sockets.client_to_server.socket.sendto(success_string, self.user.server_addr)
                    self.listen()

                # if the message and socket listen fails
                except:
                    print("client: sendall() error sending success string")
                    return

            elif data_loaded['type'] == 'query-response':

                query_long_name = input("Enter a query followed by the Long Name to the query: ")
                self.query = ' '.join(query_long_name.split()[1:])

                # print(f"Received query of {self.query}")  # Print the query received

                first_ip = data_loaded['data']['ip']

                first_port = int(data_loaded['data']['query'])

                self.connect_query_nodes(origin=self.user.query_addr, ip=first_ip, port=first_port)

                # print(response)   # Testing function

            elif data_loaded['type'] == 'join-response':
                self.joining_user = True
                self.user.username = data_loaded['data']['username']
                self.user.next_node_addr = tuple(data_loaded['data']['leader'][0])
                self.user.next_node_query_addr = tuple(data_loaded['data']['leader'][1])

                new_data = {

                    'username': self.user.username,
                    'n': 0,
                    'addr': self.user.accept_port_address,
                    'query': self.user.query_addr

                }

                # print('Teardown complete now calling reset-n\n')
                self.sockets.send_port.send_response(addr=self.user.next_node_addr, res='SUCCESS', type='reset-n', data=new_data)
            elif data_loaded['type'] == 'deregister':

                # Call end script
                self.end_script(f"{data_loaded['data']}\nTerminating client application.")

            elif data_loaded['type'] == 'leave-response':

                # set leaving user
                self.leaving_user = True

                # set success response for leaving user
                self.sockets.send_port.send_response(addr=self.user.next_node_addr, res='SUCCESS',
                                                     type='leaving-teardown')

            elif data_loaded['type'] == 'teardown-response':

                # Must be on the leader node for this operation to be successful
                if self.user.id == 0:
                    # send a successful response for teardown
                    self.sockets.send_port.send_response(addr=self.user.next_node_addr, res='SUCCESS', type='teardown')
                # Otherwise
                else:
                    # Print that the command can't be run since the node isn't the leader
                    print("\n\nCan't run this command since this is not the leader node\n")
        else:
            print(json.dumps(data_loaded, sort_keys=False, indent=4))
