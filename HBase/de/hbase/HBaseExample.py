from starbase import Connection

def read_and_load_to_hbase():
    con = Connection('master', '8000')
    ratings = con.table('ratings')

    if (ratings.exists()):
        print("Dropping existing ratings table \n")
        ratings.drop()

    ratings.create('rating')
    print('..parsing the movie data....\n')
    ratingFile = open('/home/norman/ratings_small.csv', 'r')
    batch = ratings.batch()

    for line in ratingFile:
        (userID, movieID, rating, timestamp) = line.split(',')
        batch.update(userID, {'rating' : {movieID : rating}})

    ratingFile.close()

    print("Commiting ratings data to HBase via REST service \n")
    batch.commit(finalize=True)

    print("Get back ratings for some users..\n")
    print("UserID 1 \n")
    print(ratings.fetch("1"))
    print("UserID 2 \n")
    print(ratings.fetch("2"))


if __name__ == '__main__':
    read_and_load_to_hbase()