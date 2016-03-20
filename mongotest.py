import DataService

def main():
	mongo = DataService.Mongo("benchmark")

	print("Testing get_attr()...")
	attrs = mongo.get_attr("foo", "name")
	for a in attrs:
		print(a)

	print("Testing get_pair()...")
	pairs = mongo.get_pair("foo", "name", "name")
	for pair in pairs:
		print(pair)

	print("Testing insert_one()...")
	doc = {"name": "Rachel"}
	mongo.insert_one("foo", doc);

	print("Testing insert_one()...")
	rateDoc = {"uid": 1, "ratings": [[2, 3.5], [4, 4.0], [10, 2.5]]}
	mongo.insert_one("user_rate", rateDoc);


if __name__ == "__main__":
	main()