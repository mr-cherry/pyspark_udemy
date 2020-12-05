from os.path import abspath, join

data_dir = abspath("../data")
prefix = "file://"

ACCESS_LOG = join(data_dir, "logs")
ORDERS = prefix + join(data_dir, "customer-orders.csv")
TEMPS = prefix + join(data_dir, "1800.csv")
BOOK = prefix + join(data_dir, "book.txt")
FRIENDS = prefix + join(data_dir, "fakefriends.csv")
FRIENDS_HEAD = prefix + join(data_dir, "fakefriends-header.csv")
MARVEL_GRAPH = prefix + join(data_dir, "Marvel_Graph")
MARVEL_NAME = prefix + join(data_dir, "Marvel_Names")
REALESTATE = prefix + join(data_dir, "realestate.csv")
REGRESSION = prefix + join(data_dir, "regression.txt")

MOVIE_DATA = prefix + join(data_dir, "ml-100k", "u.data")
MOVIE_ITEM = prefix + join(data_dir, "ml-100k", "u.item")
MOVIE_ITEM_SHORT = join(data_dir, "ml-100k", "u.item")
