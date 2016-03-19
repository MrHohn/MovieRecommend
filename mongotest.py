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


if __name__ == "__main__":
	main()