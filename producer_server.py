from kafka import KafkaProducer
import json
import time


class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    #TODO we're generating a dummy data
    def generate_data(self):
        input_file = open(self.input_file, 'r')
        json_load = json.load(input_file)
        for v in json_load:
            message = self.dict_to_binary(v)
            print(f"message: {message}")
            self.send(self.topic, v)
            time.sleep(1)
        #with open(self.input_file) as f:
        #    for line in f:
        #        message = self.dict_to_binary(line)
        #        # TODO send the correct data
        #        self.send()
        #        time.sleep(1)

    # TODO fill this in to return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict).encode('utf-8')

def main():
    producer_server = ProducerServer("./police-department-calls-for-service.json", 
                                     "police-department-calls-for-service", bootstrap_servers=['localhost:9092'],
                                     #value_serializer=lambda m: json.dumps(m).encode('ascii')
                                     value_serializer=lambda x: json.dumps(x).encode('utf-8')
                                    )
    producer_server.generate_data()
    
if __name__ == "__main__":
    main()