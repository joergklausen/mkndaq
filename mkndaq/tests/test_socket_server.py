# -*- coding: utf-8 -*-

"""
Tests TCP/IP communication with Picarro instrument
"""

import socket

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
    
    
def open_server_socket(HOST, PORT):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((HOST, PORT))
            s.listen()
            conn, addr = s.accept()
            with conn:
                print('Connected by', addr)
                while True:
                    data = conn.recv(1024)
                    if not data:
                        break
                    print(data)
                    conn.sendall(data)
            conn.close()
            print('Connection closed.')
    except Exception as err:
        print('Error:', err)
        
if __name__ == '__main__':
    HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
    HOST = '169.254.219.132'  # Picarro at Empa
    PORT = 51020        # Port for Picarro instrument for TCP/IP communication

    # open server socket
    open_server_socket(HOST, PORT)
    