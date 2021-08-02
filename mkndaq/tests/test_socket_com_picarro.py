# -*- coding: utf-8 -*-

"""
Tests TCP/IP communication with Picarro instrument
"""

import socket
import time

# class MySocket:
#     """demonstration class only
#       - coded for clarity, not efficiency
#     """

#     def __init__(self, sock=None):
#         if sock is None:
#             self.sock = socket.socket(
#                             socket.AF_INET, socket.SOCK_STREAM)
#         else:
#             self.sock = sock

#     def connect(self, host, port):
#         self.sock.connect((host, port))

#     def mysend(self, msg):
#         MSGLEN = 2028   # put this in because MSGLEN was not defined
#         totalsent = 0
#         while totalsent < MSGLEN:
#             sent = self.sock.send(msg[totalsent:])
#             if sent == 0:
#                 raise RuntimeError("socket connection broken")
#             totalsent += sent

#     def myreceive(self):
#         chunks = []
#         bytes_recd = 0
#         MSGLEN = 2028   # put this in because MSGLEN was not defined
#         while bytes_recd < MSGLEN:
#             chunk = self.sock.recv(min(MSGLEN - bytes_recd, 2048))
#             if chunk == b'':
#                 raise RuntimeError("socket connection broken")
#             chunks.append(chunk)
#             bytes_recd += len(chunk)
#         return b''.join(chunks)
    
    


def socket_com(HOST, PORT, cmd):
    """
    Send a command to the Picarro and receive its response.
    Picarro always ends a response with chr(13).
    
    Parameters
    ----------
    HOST : TYPE
        DESCRIPTION.
    PORT : TYPE
        DESCRIPTION.
    cmd : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    """
    try:
        response = []
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            s.sendall((cmd + chr(13) + chr(10)).encode())
  
            response = s.recv(2048)
            return(response)
    except Exception as err:
        print(err)
        
if __name__ == '__main__':
    HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
    HOST = '169.254.219.132'  # Picarro at Empa
    PORT = 51020        # Port for Picarro instrument for TCP/IP communication

    cmds = ['_Meas_GetBuffer',
            '_Meas_GetBufferFirst',
            '_Meas_GetConc',
            '_Instr_getStatus',
            '_Pulse_GetBufferFirst',
            '_Valves_Seq_Readstate',
            '_Cavity_GetPressure',
            ]
    
    for cmd in cmds:
        response = socket_com(HOST, PORT, cmd)
        print(cmd, response)

       
    
    # send command and receive response
    # for i in range(2):
    #     response = get_from_picarro(HOST, PORT, cmd)
    #     print(cmd, response)
    #     time.sleep(5)
