# used example at https://stackoverflow.com/questions/3428532/how-to-import-a-csv-file-using-python-with-headers-intact-where-first-column-is

import csv
import re
import time
import datetime
import argparse
import os
import sys
from pytz import timezone
from influxdb import InfluxDBClient
from datetime import timedelta

##
## Check if data type of field is float
##
def isfloat(value):
        try:
            float(value)
            return True
        except:
            return False

##
## Check if data type of field is int
##
def isinteger(value):
        try:
            if(float(value).is_integer()):
                return True
            else:
                return False
        except:
            return False


# parse the arguments
parser = argparse.ArgumentParser()
parser.add_argument('--batchsize', default=100000, type=int, help='How many inserts into InfluxDb in one chunk')
parser.add_argument('--filename', help='Perfmon filename to process - can be blg or csv', required=True)
parser.add_argument('--dbhost', default="localhost", help='InfluxDb hostname or ip')
parser.add_argument('--dbport', default="8086", help='InfluxDb port number')
parser.add_argument('--dbname', default="influxdb", help='InfluxDb database name')
parser.add_argument('--dbdrop', type=int, default=0, help='Drop database if exist to ensure a clean database')
parser.add_argument('--json', type=int, default=0, help='Use JSON inserts instead of line protocol')
parser.add_argument('--verbose', type=int, default=0, help='Display all parameters used in the script')

args = parser.parse_args()

if (args.verbose):    
    print("Filename=%s" %(args.filename))
    print("Batchsize=%d" %(args.batchsize))
    print("Dbhost=%s" %(args.dbhost))
    print("Dbport=%s" %(args.dbport))
    print("Dbname=%s" %(args.dbname))
    print("Dbdrop=%d" %(args.dbdrop))
    print("Json=%s" %(args.json))

# check file format
filename, extension = os.path.splitext(args.filename)

print("Extension=%s" %(extension))
if (extension == ".blg"):
    # convert to csv/perfmon file
    print( "Converting %s to %s.perfmon" %(args.filename, filename))
    command = "relog %s -y -f csv -o %s.perfmon" %(args.filename, filename)

    print(command)
    return_code = os.system(command)

    if (return_code):
        print ("Error when running %s" % (command))

        sys.exit()

    args.filename = "%s.perfmon" % (filename)



# 
datapoints = []

http_proxy  = "http://127.0.0.1:8888"
https_proxy = "http://127.0.0.1:8888"
ftp_proxy   = "http://127.0.0.1:8888"

proxyDict = { 
              "http"  : http_proxy, 
              "https" : https_proxy, 
              "ftp"   : ftp_proxy
            }

# connect to influx
client = InfluxDBClient(host=args.dbhost, port=args.dbport,proxies=proxyDict)
if (args.dbdrop):
    client.drop_database(args.dbname)

client.create_database(args.dbname)
client.switch_database(args.dbname)

# open file and read content in to csv table
file = open(args.filename,'r')
reader = csv.reader(file)
# set headers from the first row
headers = next(reader, None)

# preparete to build an in-memory table
columns = {}
for h in headers:
    columns[h] = []

# add all the values below the header
for row in reader:
    for h, v in zip(headers, row):
       columns[h].append(v) 
# dataset is now complete

# walk through the dataset, column by column
for column in columns:    
    #print (column)

    # extract the fields from the column name. With perfmon a header is like:
    #
    #\\APPSRV-SENSIGHT\Processor(_Total)\% Idle Time
    #
    #\\$host\$measurement($instance)\$field
    # 
    #
    #  First replace the percentage % symbol with 'percent'
    # TODO    
    match = re.search(r'\\\\([a-zA-Z0-9-]+)\\([a-zA-Z0-9-_ \/\[\]%]+)(?:\(([a-zA-Z0-9-_\*#:\{\}\-\. \/\\?[\]%]+)\))?\\([a-zA-Z0-9-\(\)_ \/\[\]%]+)', column)

    # start only inserting if matched, time column will be handled in the else
    if match:

        host = (match.group(1))

        measurement = (match.group(2))
        measurement = measurement.replace("Memory","win_mem")
        measurement = measurement.replace("Network Interface","win_net")
        measurement = measurement.replace("Paging File","win_swap")
        measurement = measurement.replace("LogicalDisk","win_disk")
        measurement = measurement.replace("PhysicalDisk","win_diskio")
        measurement = measurement.replace("Processor","win_cpu")
        measurement = measurement.replace("System","win_system")
        measurement = measurement.replace("Process","win_proc")
        measurement = measurement.replace("%","Percent")
        measurement = measurement.replace("/","_")
        measurement = measurement.replace(" ","_")
        measurement = measurement.replace("_sec","_persec")
        

        objectname = (match.group(2))
        objectname = objectname.replace("%","Percent")
        objectname = objectname.replace("/","_")
        objectname = objectname.replace(" ","_")
        objectname = objectname.replace("_sec","_persec")
        
        instance = (match.group(3))
        if not instance:
            instance = ""
        else:
            instance = instance.replace("%", "Percent")
            instance = instance.replace("/", "_")
            instance = instance.replace(" ", "_")
        
        field = (match.group(4))
        field = field.replace("%","Percent")
        field = field.replace("/","_")
        field = field.replace(" ","_")
        field = field.replace("_sec","_persec")

        print ('HOST %s WITH MEASUREMENT %s FOR INSTANCE %s WITH FIELD %s' %(host, measurement, instance, field,))

        # combine the value with the timestamp
        for value,timestamp in zip(columns[column], columns[headers[0]]):
            #print (value)
            # build the json object
            if (isfloat(value) or isinteger(value)):
                if (args.json):

                    datapoint =             {
                                "measurement": measurement,
                                "tags": {
                                    "host": host,
                                    "instance": instance,
                                    "objectname" : objectname
                                },
                                "time":  timestamp,
                                "fields": {
                                    field: float(value  )
                                }
                            }
                    #print(value)
                    #print (datapoint)
                    datapoints.append(datapoint)

                    #client.write_points(json_body)

                    if len(datapoints) % args.batchsize == 0:
                        #print('Read %d lines'%count)
                        print('Inserting %d datapoints...'%(len(datapoints)))
                        response = client.write_points(datapoints,  protocol ="line")

                        if response == False:
                            print('Problem inserting points, exiting...')
                            exit(1)

                        print("Wrote %d, response: %s" % (len(datapoints), response))


                        datapoints = []            
                else:
                    if instance:
                        datapoint = "%s,host=%s,instance=%s,objectname=%s %s=%d %d\n" %(measurement, host, instance, objectname, field, float(value), timestamp )
                    else:
                        datapoint = "%s,host=%s,objectname=%s %s=%d %d\n" %(measurement, host, objectname, field, float(value), timestamp )
                    datapoints.append(datapoint)

                    #print(datapoint)

                    if len(datapoints) % args.batchsize == 0:
                        #print('Read %d lines'%count)
                        print('Inserting %d datapoints...'%(len(datapoints)))
                        response = client.write_points(datapoints,  protocol ="line")

                        if response == False:
                            print('Problem inserting points, exiting...')
                            exit(1)

                        print("Wrote %d, response: %s" % (len(datapoints), response))


                        datapoints = []            

    else:
        # this it the time column, convert the timestamps from 12/24/2016 14:57:13.810 to 2018-03-28T8:01:00Z"
        print ("Asuming %s is the time column" % (column))
        
        
        match = re.search(r'\((.*?)\)\s?\((.*?)\)\((.*?)\)', column)

        importtype = (match.group(1))
        timezone = (match.group(2))
        correction = float(match.group(3))

        print ("")
        print ("Correction: %d" %(correction))
        print ("Timezone: %s" %(timezone))        

        if (args.json):
            columns[column] = [
                (datetime.datetime.strptime(timestamp,"%m/%d/%Y %H:%M:%S.%f") + timedelta(minutes=correction)).strftime("%Y-%m-%dT%H:%M:%SZ")
                for timestamp in columns[column]
                ]
        else:
            columns[column] = [
                ((datetime.datetime.strptime(timestamp,"%m/%d/%Y %H:%M:%S.%f") + timedelta(minutes=correction)).timestamp()) * 1000 * 1000 * 1000
                for timestamp in columns[column]
                ]

        


        #print ("after")

        #for timestamp in columns[column]:
        #    print (timestamp)            


 # write rest
if args.json:
    if len(datapoints) > 0:        
        print('Inserting last %d datapoints...'%(len(datapoints)))
        response = client.write_points(datapoints)

        if response == False:
            print('Problem inserting points, exiting...')
            exit(1)

        print("Wrote %d, response: %s" % (len(datapoints), response))           
else:
    if len(datapoints) > 0:
                        print('Inserting last %d datapoints...'%(len(datapoints)))
                        response = client.write_points(datapoints,  protocol ="line")

                        if response == False:
                            print('Problem inserting points, exiting...')
                            exit(1)

                        print("Wrote %d, response: %s" % (len(datapoints), response))


                        datapoints = []  
print("end")