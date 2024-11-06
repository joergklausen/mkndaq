#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test MQTT-based publication to AWS

@author: joerg.klausen@meteoswiss.ch
"""
# %%
import os
os.chdir(os.path.expanduser('~/Public/git/mkndaq'))
print(os.getcwd())

import logging
import time
import argparse
import json
#from mkndaq.utils import configparser
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

cfg = {
    # 'home': 'c:/users/jkl', 
    'aws': {
        'certPath': '~/.ssh/awsiot/kenya-mkn-minix.cert.pem',
        'privateKeyPath': '~/.ssh/awsiot/kenya-mkn-minix.private.key',
        # 'public': '~/Desktop/mkn/AWS_IoT/connect_device_package/kenya-mkn-minix.public/key',
        'rootCAPath': '~/.ssh/awsiot//root-CA.crt',
        'clientId': '0666-5077-6666',
        'host': 'a3peke9ywai7kv-ats.iot.eu-central-1.amazonaws.com',
        'port': 8883,
        'topic': 'sdk/test/Python',
        'mode': 'both',
        },
}

# Custom MQTT message callback
def customCallback(client, userdata, message):
    print("Received a new message: ")
    print(message.payload)
    print("from topic: ")
    print(message.topic)
    print("--------------\n\n")

def mqtt_message():
    msg = "Hi there. It's %s and I am alive." % time.asctime()
    msg = "It's %s." % time.asctime()
    return msg

# %%
if __name__ == "__main__":
#    cfg = configparser.expanduser_dict_recursive(cfg)

    AllowedActions = ['both', 'publish', 'subscribe']

    # Configure logging
    logger = logging.getLogger("AWSIoTPythonSDK.core")
    logger.setLevel(logging.DEBUG)
    streamHandler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    streamHandler.setFormatter(formatter)
    logger.addHandler(streamHandler)

    # Init AWSIoTMQTTClient
    # myAWSIoTMQTTClient = None
    # if useWebsocket:
    #     myAWSIoTMQTTClient = AWSIoTMQTTClient(clientId, useWebsocket=True)
    #     myAWSIoTMQTTClient.configureEndpoint(host, port)
    #     myAWSIoTMQTTClient.configureCredentials(rootCAPath)
    # else:
    myAWSIoTMQTTClient = AWSIoTMQTTClient(cfg['aws']['clientId'])
    myAWSIoTMQTTClient.configureEndpoint(cfg['aws']['host'], cfg['aws']['port'])
    myAWSIoTMQTTClient.configureCredentials(
        os.path.expanduser(cfg['aws']['rootCAPath']), 
        os.path.expanduser(cfg['aws']['privateKeyPath']), 
        os.path.expanduser(cfg['aws']['certPath']))

    # AWSIoTMQTTClient connection configuration
    myAWSIoTMQTTClient.configureAutoReconnectBackoffTime(1, 32, 20)
    myAWSIoTMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
    myAWSIoTMQTTClient.configureDrainingFrequency(2)  # Draining: 2 Hz
    myAWSIoTMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
    myAWSIoTMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec

    # Connect and subscribe to AWS IoT
    myAWSIoTMQTTClient.connect(60)
    if cfg['aws']['mode'] == 'both' or cfg['aws']['mode'] == 'subscribe':
        myAWSIoTMQTTClient.subscribe(cfg['aws']['topic'], 1, customCallback)
    time.sleep(2)

    # Publish to the same topic in a loop forever
    loopCount = 0
    while True:
        if cfg['aws']['mode'] == 'both' or cfg['aws']['mode'] == 'publish':
            message = {}
            message['message'] = mqtt_message()
            message['sequence'] = loopCount
            messageJson = json.dumps(message)
            myAWSIoTMQTTClient.publish(cfg['aws']['topic'], messageJson, 1)
            if cfg['aws']['mode'] == 'publish':
                print('Published topic %s: %s\n' % (cfg['aws']['topic'], messageJson))
            loopCount += 1
        time.sleep(1)
