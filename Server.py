'''
Daniel Quan
1216497750

CSE 434
Syrotiuk

Group: 100

Ports: 43000 - 43499

About:  Purpose of this project is to implement your own application program in which processes
    communicate using sockets to maintain a distributed hash table (DHT) dynamically, and
    answer queries using it.

ClientDriver.py:
    - This script contains a simple class that initializes a UDP socket and has some methods within that
    are very useful for the client and the server.

'''


import json
import socket
import sys

# Declare the UDPServer class
class UDPServer:

    # class instantiaition
    def __init__(self):

        # set the socket of the UDP socket
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # die_with_error function
    # Terminate the process and show an error message since the program failed
    def die_with_error(error_message):

        # Exit and print error_message
        sys.exit(error_message)

    # send_response function
    # THis function sends a response from server to client to avoid repetition
    def send_response(self, addr, res, type, data=None):

       # Set reponse data
        response_data = json.dumps({

                'res': res,
                'type': type,
                'data': data

            })

        self.socket.sendto(bytes(response_data, 'utf-8'), addr)