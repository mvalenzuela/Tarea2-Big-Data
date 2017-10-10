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
        yield [json_data["business_id"], [json_data["user_id"], json_data["stars"]]]

    def reducer_1(self, business_id, user_id_stars):
    	user_stars_list = [us for us in user_id_stars]
    	if len(user_stars_list) > 1:
    		for combination in itertools.combinations(user_stars_list, 2):
    			#sorted_combination = sorted(combination)
    			yield combination, business_id

    def mapper2(self, peer_users, business_id):
    	yield tuple([peer_users[0][0], peer_users[1][0]]), tuple([peer_users[0][1], peer_users[1][1], business_id])

    def reducer_2(self, peer_users, stars_business_id):
    	stars_business_id_list = [sb for sb in stars_business_id]
    	yield peer_users, stars_business_id_list

    def mapper3(self, peer_users, stars_business_id):
    	stars_business_id_list = [sb for sb in stars_business_id]
    	factor1 = 0
    	factor2 = 0
    	factor3 = 0
    	for sb in stars_business_id_list:
    		factor1 += (sb[0]/5.0)*(sb[1]/5.0)
    		factor2 += (sb[0]/5.0)**2
    		factor3 += (sb[1]/5.0)**2
    	factor2 = factor2**(0.5)
    	factor3 = factor3**(0.5)
    	yield peer_users, factor1/(factor2*factor3)

    def steps(self):
        return [MRStep(mapper=self.mapper_userid, reducer=self.reducer_1),
        		MRStep(mapper=self.mapper2, reducer=self.reducer_2), 
        		MRStep(mapper=self.mapper3)]

if __name__ == '__main__':
    UsersCount.run()

print("--- %s seconds ---" % (time.time() - start_time))