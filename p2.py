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
        yield json_data["user_id"], json_data["business_id"]
        
    def reducer1(self, user_id, business_ids):
        business_list = [b for b in business_ids]
        n_business = len(business_list)
        #print movie_ids
        #if n_business !=1:
        #    print n_business
        for business_id in business_list:
            yield business_id, tuple([user_id, n_business])

    def reducer_2(self, business_id, extended_userids):
        extended_userids_list = [e for e in extended_userids]
        if len(extended_userids_list) > 1:
            for combination in itertools.combinations(extended_userids_list, 2):
                sorted_combination = sorted(combination)
                yield sorted_combination, 1

    def reducer_3(self, key, values):
        n_values = sum(values)
        yield 'MAX', n_values*1.0/(key[0][1] + key[1][1]  - n_values)

    def reducer_4(self, key, values):
        yield [key,max(values)]

    def steps(self):
        return [MRStep(mapper=self.mapper_userid, reducer=self.reducer1),
                MRStep(reducer=self.reducer_2),
                MRStep(reducer=self.reducer_3),
                MRStep(reducer=self.reducer_4)]

if __name__ == '__main__':
    UsersCount.run()

print("--- %s seconds ---" % (time.time() - start_time))