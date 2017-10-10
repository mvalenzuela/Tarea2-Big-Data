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
        #if json_data["user_id"] == "natSObJ4-jEev6rJRta7jA":
        #    print json_data["text"]
        for word in json_data["text"].split(" "):
            new_word = word.replace('\\n', '')
            new_word = re.findall(r"[\w]+", new_word)
            if len(new_word) > 1:
                for sub_word in new_word:
                    yield [sub_word, json_data["user_id"]]
            else:
                yield [new_word, json_data["user_id"]]

    def reducer(self, word, users):
        user_list = [u for u in users]
        if len(user_list) == 1:
            yield [user_list[0],word]

    def reducer2(self, user_id, words):
        words_list = [w for w in words]
        yield ['MAX',[len(words_list), user_id]]

    def reducer3(self, stat, values):
        TEMP = [values]
        yield [stat, max(values)]

    def steps(self):
        return [MRStep(mapper=self.mapper_userid, reducer=self.reducer),
                MRStep(reducer=self.reducer2),
                MRStep(reducer=self.reducer3)]

if __name__ == '__main__':
    UsersCount.run()
    
print("--- %s seconds ---" % (time.time() - start_time))
