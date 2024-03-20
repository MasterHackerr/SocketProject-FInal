'''
Daniel Quan
1216497750

CSE 434
Syrotiuk

Group: 100

Ports: 43000 - 43499

ServerHeader.py:
    - This script initializes the Server and state instances and parses the commands sent in by the client

Usage:
    - python ServerDriver.py ⟨port⟩
'''

# imports
from Server import UDPServer
from state import StateInfo
import sys

BUFFER_SIZE = 1024      # initialize buffer size

# parse_data function
# Function parses messages sent to the server and will call the corresponding functions
# If the command doesn't match any in the parser database, an error will be thrown


def parse_data(server, state, data, address):

    if data:
        print(f"server: received string ``{data.decode('utf-8')}'' from client on ip: {address[0]} port {address[1]}\n")
        data_list = data.decode('utf-8').split()
        command = data_list[0]

        # If DHT is NOT complete after creation
        if state.creating_dht and command != 'dht-complete':
            server.send_response(addr=address, res='FAILURE', type='error', data='Creating DHT')

        # If the DHT is NOT stabilized after rebuilding
        elif state.stabilizing_dht and command != 'dht-rebuilt':
            server.send_response(addr=address, res='FAILURE', type='error', data='Stabilizing DHT')

        # Register response
        elif command == 'register':
            res, err = state.register(data_list)

            # Failure response
            if err:
                server.send_response(addr=address, res='FAILURE', type='register-error', data=err)
            else:
                server.send_response(addr=address, res='SUCCESS', type='register', data=res)
        # Setup DHT response
        elif command == 'setup-dht':

            # setup-dht ⟨n⟩ ⟨user-name⟩

            if state.dht_flag:
                server.send_response(addr=address, res='FAILURE', type='setup-dht', data='DHT already created')
            else:
                # Make call to setup_dht
                res, err = state.setup_dht(data_list)

                # Failure response
                if err:
                    server.send_response(addr=address, res='FAILURE', type='DHT-error', data=err)
                else:
                    server.send_response(addr=address, res='SUCCESS', type='DHT', data=res)

        # Deregister response
        elif command == 'deregister':

            res, err = state.deregister(data_list)

            # Failure response
            if err:
                server.send_response(addr=address, res='FAILURE', type='deregister-error', data=err)
            else:
                server.send_response(addr=address, res='SUCCESS', type='deregister', data=res)

        # query DHT response
        elif command == 'query-dht':

            res, err = state.valid_query(data_list)

            # Failure response
            if err:
                server.send_response(addr=address, res='FAILURE', type='query-error', data=err)
            else:
                server.send_response(addr=address, res='SUCCESS', type='query-response', data=res)

        # DHT complete response
        elif command == 'dht-complete':

            if data_list[1] == state.dht_leader:

                if state.creating_dht:
                    state.creating_dht = False
                    server.send_response(addr=address, res='SUCCESS', type='dht-setup')
                # Failure response
                else:
                    server.send_response(addr=address, res='FAILURE', type='dht-setup-error', data="DHT is not currently being created")
            else:
                server.send_response(addr=address, res='FAILURE', type='dht-setup-error',data=f"{state.dht_leader} is the DHT leader, not {data_list[1]}")

        # join DHT response
        elif command == 'join-dht':

            res, err = state.join_dht(data_list)

            # Failure response
            if err:
                server.send_response(addr=address, res='FAILURE', type='join-error', data=err)
            else:
                server.send_response(addr=address, res='SUCCESS', type='join-response', data=res)

        # leave DHT response
        elif command == 'leave-dht':
            res, err = state.leave_dht(data_list)

            # Failure response
            if err:
                server.send_response(addr=address, res='FAILURE', type='leave-error', data=err)
            else:
                server.send_response(addr=address, res='SUCCESS', type='leave-response', data=res)

        # dht rebuilt response
        elif command == 'dht-rebuilt':
            res, err = state.dht_rebuilt(data_list)

            # Failure response
            if err:
                server.send_response(addr=address, res='FAILURE', type='rebuilt-error', data=err)
            else:
                server.send_response(addr=address, res='SUCCESS', type='rebuilt-response', data=res)

        # DHT teardown response (teardown initialized)
        elif command == 'teardown-dht':
            res, err = state.teardown_dht(data_list)

            # Failure response
            if err:
                server.send_response(addr=address, res='FAILURE', type='teardown-error', data=err)
            else:
                server.send_response(addr=address, res='SUCCESS', type='teardown-response', data=res)

        # Teardown complete response (teardown finished)
        elif command == 'teardown-complete':

            res, err = state.teardown_complete(data_list)

            # Failure response
            if err:
                server.send_response(addr=address, res='FAILURE', type='teardown-complete-error', data=err)
            else:
                server.send_response(addr=address, res='SUCCESS', type='teardown-complete', data=res)

        # Display users response
        elif command == 'display-users':

            state.display_users()
            server.send_response(addr=address, res='SUCCESS', type='debugging')

        # Display dht response
        elif command == 'display-dht':

            state.display_dht()
            server.send_response(addr=address, res='SUCCESS', type='debugging')

        # Failure response
        else:
            server.send_response(addr=address, res='FAILURE', type='error', data='Unkown command')

    # return the final server and state status
    return server, state


# Usage: python ServerHeader.py
def main(args):

    if len(args) != 2:
        sys.exit(f"Usage:  {args[0]} <UDP SERVER PORT>\n")

    server_port = int(args[1])  # First arg: Use given port

    server = UDPServer()                # Set server
    state = StateInfo(server_port)      # Set the state with the server port

    try:
        server.socket.bind(("", server_port))
    except:
        server.die_with_error("server: bind() failed")

    print(f"server: Port server is listening to is: {server_port}\n")

    # Add loop here so that we can disconnect and reconnect to server
    while True:

        message, addr = server.socket.recvfrom(BUFFER_SIZE)

        # print(f"server connected by addr: {addr}")

        server, state = parse_data(server, state, message, addr)


if __name__ == "__main__":
    main(sys.argv)