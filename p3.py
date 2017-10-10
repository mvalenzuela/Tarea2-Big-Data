from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import re
import itertools
import json
import time

start_time = time.time()

class UsersCount(MRJob):
    def mapper_userid(self, _, line):
        json_data = json.loads(line)
        if len(json_data) == 15:
            yield json_data["business_id"], ["categories_tag", json_data["name"], json_data["categories"]]
        else:
            yield json_data["business_id"], [json_data["user_id"], json_data["useful"]]
    
    def reducer_1(self, business_id, data):
        data_list = [d for d in data]
        categories_list = []
        users = []
        if len(data_list) > 1:
            for data in data_list:
                if data[0] == "categories_tag":
                    categories_list = data[2]
                else:
                    users.append(data)
            for category in categories_list:    
                yield category, users

    def mapper2(self, category, user_info):
        for info in user_info:
            yield [category, info[0]], info[1]

    def reducer_2(self, category_user, useful):
        useful_list = [u for u in useful]
        yield category_user[0], [category_user[1], sum(useful_list)]

    def reducer_3(self, category, user_relevant_value):
        user_relevant_value_list = [u for u in user_relevant_value]
        max_value = 0
        user_with_max_value = ""
        for user_relevant_value in user_relevant_value_list:
            if user_relevant_value[1] > max_value:
                max_value = user_relevant_value[1]
                user_with_max_value = user_relevant_value[0]
        yield [category, user_with_max_value], max_value   

    def steps(self):
        return [MRStep(mapper=self.mapper_userid, reducer=self.reducer_1),
                MRStep(mapper=self.mapper2, reducer=self.reducer_2),
                MRStep(reducer=self.reducer_3)]

if __name__ == '__main__':
    UsersCount.run()

print("--- %s seconds ---" % (time.time() - start_time))