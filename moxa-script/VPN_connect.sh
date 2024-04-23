user=$1
pass=$2
host=$3
echo $pass | openconnect -s /etc/vpnc/vpnc-script $host -u $user -b --passwd-on-stdin
return 1
