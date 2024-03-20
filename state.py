'''
Daniel Quan
1216497750

CSE 434
Syrotiuk

Group: 100

Ports: 43000 - 43499

state.py:
    - This script keeps the state table and handles the commands given from the client

'''

# imports
import json
import random

# StateInfo class
class StateInfo:

    # Instantiation of StateInfo and all information that comes with it
    def __init__(self, port):
        self.state_table = {}  # Initialize an empty dictionary for the state table
        self.server_port = port
        self.ports = [port]
        self.dht_flag = False
        self.creating_dht = False
        self.stabilizing_dht = False
        self.tearing_down_dht = False
        self.dht_leader = None
        self.leaving_user = None

    # User
    # Amount of users instantiated will match the amount of users registered
    class User:

        def __init__(self, username, ip_address, ports):
            self.username = username
            self.ipv4 = ip_address
            # Convert port to integers
            self.client_port = int(ports[0])
            self.client_query_port = int(ports[1])
            self.state = 'Free'

    # reset_dht function
    def reset_dht(self):

        # Set every user state to 'Free'
        for username in self.state_table.keys():
            self.state_table[username].state = 'Free'

        # Set all dht properties to default
        self.dht_flag = False
        self.tearing_down_dht = False
        self.dht_leader = None
        self.ports = [self.server_port]

    # valid_user function
    # Function that checks if the given user is valid for being registered
    def valid_user(self, user):

        # Checks if username already exists
        # Checks also if the username is all alphabetical (if not, it's invalid)
        if user in self.state_table.keys():
            return "User already registered"

        if not user.isalpha():
            return "Username must be an alphabetic string"

        return None

    # register function
    # Takes a command from the client and checks if the given info isvalid for registering a new user
    # ex: register ⟨username⟩ ⟨IP⟩ ⟨acceptance port⟩ ⟨query port⟩
    def register(self, data_list):

        if len(data_list) != 5:
            return None, "Invalid number of arguments passed - expected 5"

        if len(data_list[1]) > 15:
            return None, "Username is too long, character limit is 15"

        # Check if the ports are currently taken
        for port in data_list[3:]:
            if port in self.ports:
                return None, f"Port {port} already taken"

        err = self.valid_user(data_list[1])
        if err:
            return None, err

        # Add the ports to the list of ports
        # The added ports are now reserved
        self.ports.append(data_list[3])
        self.ports.append(data_list[4])

        user = self.User(data_list[1], data_list[2], data_list[3:])
        self.state_table[user.username] = user

        # Print that the port was added to the state table successfully
        return f"{data_list[1]} added to state table successfully", None

    # deregister function
    # Function will check if the user that's going to deregister has the proper and valid info supplied by the client
    # This function then removes the user from the state table
    # ex: deregister <username>
    def deregister(self, data_list):

        # Invalid number of arguments
        if len(data_list) != 2:
            return None, "Invalid number of arguments - expected 2"

        # The user that's been designated for de-registration is not in the state table
        if data_list[1] not in self.state_table.keys():
            return None, f"{data_list[1]} not in state table"

        user_to_deregister = self.state_table[data_list[1]]

        # If the user designated for de-registration was not in the free state
        if user_to_deregister.state != 'Free':
            return None, "User given is not in Free state"
        else:
            self.state_table[user_to_deregister.username].registered = False
            del self.state_table[user_to_deregister.username]

        # Print that the user was successfully removed from the state table
        return "Successfully removed user from state table", None

    # setup_dht function
    # Set up the local server DHT and the three tuples within the server
    # Updates the state table's state information
    # ex: setup-dht ⟨n⟩ ⟨username⟩
    def setup_dht(self, data_list):

        # Invalid number of arguments in the 3 tuple
        if len(data_list) != 3:
            return None, "Invalid number of arguments - expected 3"

        # Not in the state table
        if data_list[2] not in self.state_table.keys():
            return None, f"{data_list[2]} not in state table"

        n = int(data_list[1])

        if n < 2 or n > len(self.state_table):
            return None, f"Invalid n value -> {n}"

        setup_dht_response = []
        self.dht_leader = data_list[2]

        # Start the id at 1 because the leader has an id of 0
        dht_id = 1

        # Setting up the local State Table, server response message, and updating state_table
        for key, value in self.state_table.items():
            if key == data_list[2]:
                self.state_table[key].state = 'Leader'
                setup_dht_response.insert(0, {
                    'n': n,
                    'id': 0,
                    'username': value.username,
                    'ip': value.ipv4,
                    'port': value.client_port,
                    'query': value.client_query_port
                })
            elif value.state != 'InDHT' and dht_id != n:
                self.state_table[key].state = 'InDHT'
                setup_dht_response.append({
                    'n': n,
                    'id': dht_id,
                    'username': value.username,
                    'ip': value.ipv4,
                    'port': value.client_port,
                    'query': value.client_query_port
                })
                dht_id += 1

        self.dht_flag = True
        self.creating_dht = True

        return setup_dht_response, None

    # valid_query function
    # Checks if the query command is valid, sends a response, and sends information on a random user that the cleint will use to commence a query search
    # ex: query <username>
    def valid_query(self, data_list):

        # invalid number of arguments
        if len(data_list) != 2:
            return None, "Invalid number of arguments - expected 2."

        # There was no DHT created
        if not self.dht_flag:
            return None, "There is no DHT created"

        # if the user designated is not registered with the server
        if data_list[1] not in self.state_table.keys():
            return None, f"{data_list[1]} is not registered with the server"

        # If the user designated user is currently managing the DHT and is therefore NOT Free
        if self.state_table[data_list[1]].state != 'Free':
            return None, f"{data_list[1]} is currently maintaining the DHT. Only free users can query the DHT."

        # All prior checks have been passed, so this is a valid query command
        maintainers = ['Leader', 'InDHT']
        dht_maintainers = [user for user in self.state_table.values() if user.state in maintainers]
        random_user_index = random.randrange(len(dht_maintainers))
        random_user = dht_maintainers[random_user_index]
        random_user = {

            'username': random_user.username,
            'ip': random_user.ipv4,
            'query': random_user.client_query_port

        }

        return random_user, None

    # join_dht function
    # Checks if join dht command is valid
    # If the command is valid, then send a response with the information on the current leader of the DHT
    # ex: join-dht <username>
    def join_dht(self, data_list):

        # Invalid number of arguments in the command
        if len(data_list) != 2:
            return None, "Invalid number of arguments - expected 2."

        # THere is no DHT
        if not self.dht_flag:
            return None, "There is no DHT created"

        join_data = {
            'username': None,
            'leader': None,
        }

        # The user is not registered with the server
        if data_list[1] not in self.state_table.keys():
            return None, f"{data_list[1]} is not registered with the server."

        # The designated user is maintaining the DHT and is NOT Free
        if self.state_table[data_list[1]].state != 'Free':
            return None, f"{data_list[1]} is already maintaining the DHT."

        # Valid user has been given
        self.joining_user = data_list[1]
        self.stabilizing_dht = True
        join_data['username'] = data_list[1]
        leader = self.state_table[self.dht_leader]
        join_data['leader'] = [(leader.ipv4, leader.client_port), (leader.ipv4, leader.client_query_port)]

        return join_data, None

    # leave_dht function
    def leave_dht(self, data_list):

        # invalid number of arguments in the command
        if len(data_list) != 2:
            return None, "Invalid number of arguments - expected 2."

        # No DHT has been created
        if not self.dht_flag:
            return None, "There is no DHT created"

        maintainers = ['Leader', 'InDHT']
        dht_maintainers = [user for user in self.state_table.values() if user.state in maintainers]

        if (len(dht_maintainers)) < 2:
            return None, "Current DHT doesn't have enough maintainers for anyone to leave"

        if data_list[1] not in self.state_table.keys():
            return None, f"{data_list[1]} is not registered with the server."

        # The selected user is currently maintaining the DHT, is NOT Free and can't be used
        if self.state_table[data_list[1]].state == 'Free':
            return None, f"{data_list[1]} is not currently maintaining the DHT"

        # Valid user given
        self.leaving_user = data_list[1]
        self.stabilizing_dht = True
        return f"Removing {data_list[1]} from DHT", None

    # dht_rebuilt
    def dht_rebuilt(self, data_list):

        # invalid number of arguments
        if self.leaving_user:
            if len(data_list) != 3:
                return None, "Invalid number of arguments - expected 3."

            if data_list[1] != self.leaving_user:
                return None, "Only the user who initiated the leave-dht can respond with complete"

            for user, value in self.state_table.items():
                if value.state == 'Leader':
                    value.state = 'InDHT'
                    self.state_table[user] = value

            self.state_table[self.leaving_user].state = 'Free'
            self.state_table[data_list[2]].state = 'Leader'

            self.dht_leader = data_list[2]
            self.stabilizing_dht = False
            self.leaving_user = None

            return "DHT has been successfully rebuilt", None

        # Invalid number of arguments
        elif self.joining_user:
            if len(data_list) != 2:
                return None, "Invalid number of arguments - expected 2."

            # If a user which didn't initiate the join-dht attempts to respond complete
            if data_list[1] != self.joining_user:
                return None, "Only the user who initiated the join-dht can respond with complete"

            # Updating the state of new DHT maintainer
            self.state_table[data_list[1]].state = 'InDHT'

            self.stabilizing_dht = False
            self.joining_user = None

            return "DHT has been successfully rebuilt", None

        else:
            return "There is no dht-rebuild in process", None

    # teardown_dht function
    def teardown_dht(self, data_list):

        # Check if the teardown_dht command has the correct amount of arguments
        if len(data_list) != 2:
            return None, "Invalid number of arguments - expected 2."

        # If there was no DHT created
        if not self.dht_flag:
            return None, "There is no DHT created"

        # if the user isn't in the state table
        if data_list[1] not in self.state_table.keys():
            return None, f"{data_list[2]} not in state table"

        # If the user attempting to teardown the DHT is NOT the leader, therefore the DHT won't be torn down
        if self.state_table[data_list[1]].state != 'Leader':
            return None, f"{data_list[1]} is not the leader of the DHT"

        # Set this flag so server knows it is in busy state and indicate the start of the DHT teardown
        self.tearing_down_dht = True
        return f"Initiating teardown of the DHT", None

    # teardown_complete function
    def teardown_complete(self, data_list):

        # Check if teardown_complete has the correct number of arguments
        if len(data_list) != 2:
            return None, "Invalid number of arguments - expected 2."

        # If a user other than the dht leader attempts to check on the dht teardown process
        if data_list[1] != self.dht_leader:
            return None, "Only the DHT leader can send this command"

        # Teardown operation wasn't started from teardown_dht
        if not self.tearing_down_dht and not self.stabilizing_dht:
            return None, "The DHT is not being torn down"

        # dht teardown was successful
        self.reset_dht()
        return "Successfully destroyed DHT", None

    # For debugging
    def display_users(self):
        i = 1
        print("\nDisplaying all users in Server state table: ")
        for username, value in self.state_table.items():
            print(f"\n\t{i}:\t{username}\n\t")
            print(json.dumps(vars(value), sort_keys=False, indent=4))
            i += 1