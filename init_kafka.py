import os

global kafka_folder

# Download Kafka :

if any(x.startswith('kafka') for x in os.listdir('./')):
    kafka_folder = [filename for filename in os.listdir('.') if filename.startswith("kafka")][0].split('.')[0]
    print(kafka_folder)
else:
    print('kafka downloading...')
    get_kafka = 'wget http://apache.crihan.fr/dist/kafka/2.2.0/kafka_2.11-2.2.0.tgz'
    kafka_archive = [filename for filename in os.listdir('.') if filename.startswith("kafka")][0]
    print(kafka_archive)
    unzip = 'tar xzf {}'.format(kafka_archive)
    kafka_folder = kafka_archive.split('.')[0]
    os.system(get_kafka, unzip)

# 1. Launch zookeeper
# 2. Launch Kafka broker
# 3. Creation Topic

os.chdir('./{}'.format(kafka_folder))

cmd1 ='./bin/zookeeper-server-start.sh ./config/zookeeper.properties'
cmd2 = './bin/kafka-server-start.sh ./config/server.properties'
cmd3 = './bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic velib-stations' # --consumer-property group.id=mygroup

def launching_kafka():
    print('Launching zookeeper => option 1')
    print('Launching kafka broker => option 2')
    print('Create topic velib-stations => option 3')
    option = str(input('Tape 1, 2 or 3 to select your choice or anything else to quit : '))
    if option == '1':
        try:
            os.system(cmd1)
            print('zookeeper launched successfully')
        except:
            print('error as zookeeper launching')
    elif option == '2':
        try:
            os.system(cmd2)
            print('broker kafka launched successfully')
        except:
            print('error as broker kafka launching')
    elif option == '3':
        try :
            os.system(cmd3)
            print('topic velib-stations created successfully')
        except:
            print('error as topic creating')
    else:
        print('Quitting...')
        pass


if __name__ == "__main__":
    launching_kafka()
