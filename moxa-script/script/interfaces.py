import sys
sys.path.insert(0,'/home/admin')
import version
import os

network_file = '/etc/network/interfaces'
info_eth0 = '# embedded ethernet LAN1'
info_eth1 = '# embedded ethernet LAN2'
info_lo = '# embedded ethernet LOCAL'

new_interfaces = 'auto eth0\niface eth0 inet dhcp\n'
cmd = """sudo perl -0777 -i -pe "s/(%s\\n).*(\\n%s)/\$1%s\$2/s" %s""" %(info_eth0,info_eth1,new_interfaces,network_file)
os.system(cmd)

new_interfaces="auto eth1\niface eth1 inet static\n\taddress 10.182.255.1\n\tnetmask 255.255.255.0\n\tnetwork 10.182.255.0\n\tbroadcast 10.182.255.255\n"
cmd = """sudo perl -0777 -i -pe "s/(%s\\n).*(\\n%s)/\$1%s\$2/s" %s""" %(info_eth1,info_lo,new_interfaces,network_file)
os.system(cmd)