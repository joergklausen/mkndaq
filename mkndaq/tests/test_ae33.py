# %%
import os
import socket
import time

# configure tcp/ip
_sockaddr = ("192.168.3.137", 8002)
_socktout = 1
_socksleep = 0.5

def tcpip_comm(cmd: str, tidy=True) -> str:
    """
    Send a command and retrieve the response. Assumes an open connection.

    :param cmd: command sent to instrument
    :param tidy: 
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
                try:
                    data = s.recv(1024)
                    rcvd = rcvd + data
                except:
                    break

        # decode response, tidy
        rcvd = rcvd.decode()
        if tidy:
            # rcvd = rcvd.replace("\n", "").replace("\r", "").replace("AE33>", "")
            rcvd = rcvd.replace("AE33>", "")
            rcvd = rcvd.replace("\r\n", "\n")
            rcvd = rcvd.replace("\n\n", "\n")
        return rcvd

    except Exception as err:
         print(err)


# %%
import time
def print_ae33_data(data=None) -> None:
    if data is None:
        data = "AE33-S10-01394|30295|10/21/2022 5:57:00 PM|10/21/2022 5:58:00 PM|11|8/1/2022 5:49:14 AM|955850|821777|853984|946852|835965|874671|961233|809886|837586|940695|844702|864563|956256|900621|933770|769761|903286|939664|848753|926817|956551|657|455|658|659|704|660|647|698|648|634|882|635|619|814|620|536|664|537|601|741|602|0.004901551|0.005432921|0.005658355|0.00585455|0.006012386|0.006273077|0.006502224|9.6|101325.0|21.1|3843|1149|4992|29.0|41.0|29.0|0|10|10|0|0|51|290|0|30435|1"
    __name = "ae33"
    itms = data.split("|")
    
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{__name} S/N: {itms[0]}] {data}")

print(print_ae33_data())


# %%
