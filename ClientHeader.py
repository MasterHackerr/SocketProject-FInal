'''
Daniel Quan
1216497750

CSE 434
Syrotiuk

Group: 100

Ports: 43000 - 43499

ClientHeader.py:
    This file manages the clients and accepts user input to pass to the instance of the client
    and interact with server functions

Usage:
    -- S = Server ; C = Client ; P = Port

    python ClientDriver.py ⟨username⟩ ⟨S IP⟩ ⟨S P⟩ ⟨C IP⟩ ⟨C left P⟩ ⟨C query P⟩ ⟨C accept P⟩
'''

from Client import Client
import sys
import time

# Initialize constants
HASH_SIZE = 353  # Size to initialize the local hash table to
BUFFER_SIZE = 4096  # Max bytes to take in
FILE_PATH = "details-1996.csv"      # Set the file path
ALL_COMMANDS = ['join-dht', 'leave-dht', 'query-dht', 'register', 'deregister', 'setup-dht', 'teardown-dht']    # Declare all commands
DEBUGGING_COMMANDS = ['check-node', 'help', 'display-users']        # Declare debugging commands
BASIC_COMMANDS = ['join-dht', 'leave-dht', 'query-dht', 'deregister', 'teardown-dht']       # Declare basic commands

'''
All Commands:

    join-dht: Command will adds the user to the existing DHT.
    
    leave-dht: Leaves the dht
    
    query-dht: Command is used to initiate a query of DHT.
    
    register: This command registers a new user with the server. All users must be registered prior to issuing other any other commands to the server.
    
    deregister: This command removes the state of the user from the state information base (if currently Free), allowing it to terminate.
        
    setup-dht ⟨n⟩: Where n >= 2. This command initiates the construction of a DHT of size n, with user-name as its leader. Only one DHT may exist at one time.

    teardown-dht: The current user is the leader of the DHT. This command commences the deletion of the DHT.
        
Debugging Commands:

     check-node: This command outputs important info about the current node

     display-users: Displays the database and the users
'''


def read_input(client):

    while True:
        # A time delay for when one can enter next command
        time.sleep(0.2)
        user_input = input("\nEnter command for the server: ")
        data_list = user_input.split()
        command = data_list[0]

        # If the command is in ALL_COMMANDS
        if command in ALL_COMMANDS:

            string_to_serv = user_input

            # If the command is in BASIC_COMMANDS
            if command in BASIC_COMMANDS:
                string_to_serv = f'{command} {client.user.username}'

            elif command == 'register':

                # register Dan 127.0.0.1 64352 64330
                string_to_serv = f'{command} {client.user.username} {client.user.accept_port_address[0]} {client.user.accept_port_address[1]} {client.user.query_addr[1]}'

            elif command == 'setup-dht':

                if len(data_list) > 1:

                    string_to_serv = f'{command} {data_list[1]} {client.user.username}'

                else:
                    string_to_serv = f'{command}'

            # Send command to server
            string_to_serv = bytes(string_to_serv, 'utf-8')

            # Try to send message to the server through the socket and listen
            try:
                client.sockets.client_to_server.socket.sendto(string_to_serv, client.user.server_addr)
                client.listen()

            # Otherwise print an error
            except Exception as error:
                print(error)
                print("client: sendall() error")

        # If the command is in DEBUGGING_COMMANDS
        elif command in DEBUGGING_COMMANDS:

            # Check node command
            if command == 'check-node':
                client.output_node_info()

            # Help command
            elif command == 'help':
                print(read_input.__doc__)

            # Display DHT command
            elif command == 'display-dht' or command == 'display-users':
                client.sockets.client_to_server.socket.sendto(bytes(command, 'utf-8'), client.user.server_addr)

        # If the command matches none of the commands listed above, the command is invalid
        else:
            print("Invalid command! Send help if you need to see all valid commands.\n")


'''
    Usage:
        -- S = Server ; C = Client ; P = Port

        python ClientHeader.py ⟨username⟩ ⟨S IP⟩ ⟨S P⟩ ⟨C IP⟩ ⟨C left P⟩ ⟨C query P⟩ ⟨C accept P⟩
'''
def main(args):

    if not (len(args) == 8):
        print(f"Incorrect number of arguments\n\n", main.__doc__)
        quit()

    username = args[1]
    serv_IP = args[2]  # First arg: server IP address (dotted decimal)
    echo_serv_port = int(args[3])  # Second arg: Use given port
    client_IP = args[4]     # client IP
    client_port = int(args[5])      # client port
    query_port = int(args[6])       # query port
    right_port = int(args[7])       # right port

    client = Client(username, serv_IP, echo_serv_port, client_IP, client_port, query_port, right_port, HASH_SIZE, BUFFER_SIZE, FILE_PATH)

    client.start_threads()

    read_input(client)


if __name__ == "__main__":
    main(sys.argv)
