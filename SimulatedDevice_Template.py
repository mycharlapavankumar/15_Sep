# program sends FieldId, Date, GatewayId, DeviceId to IoT Hub
#


import random
import time

from azure.iot.device import IoTHubDeviceClient, Message

CONNECTION_STRING =""
MSG_TXT='{{"FieldId":"{FieldId}","Date":{Date},"GatewayId":"{GatewayId}","DeviceId":"{DeviceId}"}}'
 
def iothub_client_init():
    client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)
    return client

def iothub_client_telemetry_sample_run():

    try:
        client = iothub_client_init()
        input1 = open('input.csv', 'r')
        
        print ( "IoT Hub device sending periodic messages, press Ctrl-C to exit" )
        messageId=0

        Lines = input1.readlines()
        for line in Lines:
            print(line)
            x = line.split(',')
                 
            FieldId=str(x[0])
            Date=x[1]
            GatewayId = str(x[2])
            DeviceId=str(x[3])


            msg_txt_formatted = MSG_TXT.format(FieldId=FieldId,Date=Date,
                                GatewayId=GatewayId,DeviceId=DeviceId)
            message = Message(msg_txt_formatted)
          
            print( "Sending message: {}".format(message) )
            client.send_message(message)
            print(message)
            print ( "Message successfully sent" )
            time.sleep(3)

    except OSError as e:
            print('Error: ' + str(e))
            
if __name__ == '__main__':
    print ( "IoT Hub Simulated device Template" )
    print ( "Press Ctrl-C to exit" )
    iothub_client_telemetry_sample_run()
