# %%
import os
import socket
import time

# configure tcp/ip
_sockaddr = ("192.168.0.53", 8002)
_socktout = 1
_socksleep = 0.5

def tcpip_comm(cmd: str, tidy=True) -> str:
    """
    Send a command and retrieve the response. Assumes an open connection.

    :param cmd: command sent to instrument
    :param tidy: remove cmd echo, \n and *\r\x00 from result string, terminate with \n
    :return: response of instrument, decoded
    """
    rcvd = b''
    try:
        # open socket connection as a client
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM, ) as s:
            # connect to the server
            s.settimeout(_socktout)
            s.connect(_sockaddr)

            # send data
            s.sendall((cmd + chr(13) + chr(10)).encode())
            time.sleep(_socksleep)

            # receive response
            while True:
                data = s.recv(1024)
                rcvd = rcvd + data
                if chr(13).encode() in data:
                    break

        # decode response, tidy
        rcvd = rcvd.decode()
        if tidy:
            rcvd = rcvd.replace("\n", "").replace("\r", "").replace("AE33>", "")

        return rcvd

    except Exception as err:
        # if cls._log:
        #     cls._logger.error(err)
        print(err)

def FetchFromTable(name: str, rows=None, first=None, last=None) -> str:
    try:
        if name is None:
            raise("Table 'name' must be provided.")
        if first is None:
            if last is None:
                if rows is None:
                    # fetch all data from table
                    cmd = f"FETCH {name} 1"
                else:
                    # fetch number of rows from end of table
                    maxid = int(tcpip_comm(cmd=f"MAXID {name}", tidy=True))
                    cmd=f"FETCH {name} {maxid-rows}"                    
            elif rows is None:
                raise("Number of 'rows' to read must be provided together with 'last'.")
            else:
                # fetch number of rows up until last
                cmd = f"FETCH {name} {last-rows} {last}"
        elif last is None:
            if rows is None:
                # fetch all data starting at first
                cmd = f"FETCH {name} {first}"
            else:
                # fetch number of rows starting at first
                cmd = f"FETCH {name} {first} {first+rows}"
        else:
            if rows is None:
                cmd = f"FETCH {name} {first} {last}"
            else:
                raise("Ambiguous request, cannot use all of 'first', 'last' and 'rows' at once.")
        
        resp = tcpip_comm(cmd=cmd, tidy=False)


        #     elif rows is None:
                
        #     else: 
        # elif rows is None:
        # else:
        #     resp = tcpip_comm(cmd=f"FETCH {name} {first} {first+rows}", tidy=False)

                    # get maxid and fetch number of rows from end
                #     maxid = tcpip_comm(cmd=f"MAXID {name}", tidy=False)
                #     cmd = f"FETCH {name} {maxid-rows}"
                # if rows is None:
                #     raise("Number of 'rows' to read must be provided together with 'first'.")
                # else:

        return cmd, resp
    except Exception as err:
        print(err)

def main():
    file = "AE33-test.txt"
    cmds = ["HELLO", 
            "FETCH ClassName",
            "FETCH DateOrder",
            "MINID Message",
            "MAXID Message",
            "FETCH Message 1",
            "FETCH Method",
            "MINID NDTest",
            "MAXID NDTest",
            "FETCH NDTest 1"
            "MINID Data", 
            "MAXID Data", 
            "FETCH Data 30101 30109",
            "FETCH Data 30101"
            "MINID FilterSet",
            "MAXID FilterSet",
            "FETCH FilterSet 1",
            "MINID Log", 
            "MAXID Log", 
            "FETCH Log 601 681",
            f"$AE33:{time.strftime('T%Y%m%d%H%M%S')}"]

    with open(file, "w") as fh:
        for cmd in cmds:
            resp = tcpip_comm(cmd, tidy=False)
            print(f"## {cmd}\n{resp}")
            fh.write(f"## {cmd}\n{resp}")
        fh.close()

    tables = ["ClassName",
              "Data",
              "DateOrder",
              "ExtDeviceData",
              "ExtDeviceSetup",
              "FilterSet",
              "Log",
              "Message",
              "Method",
              "NDTest",
              "Setup",
              "TestReports"]

    file = "AE33-test2.txt"
    with open(file, "w") as fh:
        for tbl in tables:
            cmd, resp = FetchFromTable(name=tbl, rows=10)
            print(f"## {cmd}\n{resp}")
            fh.write(f"## {cmd}\n{resp}")
        fh.close()

    file = "AE33-test3.txt"
    with open(file, "w") as fh:
        for tbl in tables:
            cmd, resp = FetchFromTable(name=tbl)
            print(f"## {cmd}\n{resp}")
            fh.write(f"## {cmd}\n{resp}")
        fh.close()

if __name__ == "__main__":
    main()