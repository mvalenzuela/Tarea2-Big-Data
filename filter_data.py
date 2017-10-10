import json

number_of_reviews_desired = 30000

business_ids_list = []

review_file=open("review.json","r")
business_file=open("business.json", "r")
filtered_review_file=open("review_filtered.json","w")
filtered_business_file=open("business_filtered.json", "w")

for i in range(number_of_reviews_desired):
	line = review_file.readline()
	json_data = json.loads(line)
	if json_data["business_id"] not in business_ids_list:
		business_ids_list.append(json_data["business_id"])
	filtered_review_file.write(line)

for line in business_file.readlines():
	json_data = json.loads(line)
	if json_data["business_id"] in business_ids_list:
		filtered_business_file.write(line)

#print business_ids_list

filtered_review_file.close()
filtered_business_file.close()
