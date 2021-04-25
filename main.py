import sys
from pyspark import SparkContext, SparkConf, SQLContext

sc = SparkContext("local", "First App")

sqlCtx = SQLContext(sc)
amzDF = sqlCtx.jsonFile("project/amazon-meta.small.json")
amzDF.registerTempTable("purchases")
print('---- Schema')
amzDF.printSchema()
print('-/-- Schema')

# Arguments
## 1 - search program

try:

    if len(sys.argv) <= 1:
        raise Exception ('Argument 1, utility name, must be specified')

    if sys.argv[1] == 'search-title':

        # Arguments
        ## 2 - query string

        if len(sys.argv) < 3:
            # todo - should throw an error
            print('ERRORR!!!!!!')

        ## Search by title, partial matching allowed, case insensitive
        searchTitles = sqlCtx.sql(""" SELECT * FROM purchases WHERE lower(title) LIKE "%{}%" """.format(sys.argv[2]))
        for i in searchTitles.collect(): print("Matched Product: " + i.Id + ", title: " + i.title)


    if sys.argv[1] == 'search-review-count':

        # Arguments
        ## 2 - operator, eg. >, >=, =, <, <=
        ## 3 - count

        if len(sys.argv) < 4:
            # todo - should throw an error
            print('ERRORR!!!!!!')

        ## Search by X review count
        searchReviewCount = sqlCtx.sql(" SELECT * FROM purchases WHERE reviews.total {} {} ".format(sys.argv[2], sys.argv[3]))
        for i in searchReviewCount.collect(): print ("Matched Product: " + i.Id + ", Reviews: " + i.reviews.total + ", title: " + i.title)

    if sys.argv[1] == 'search-review-avg-rating':

        # Arguments
        ## 2 - operator, eg. >, >=, =, <, <=
        ## 3 - avg rating

        if len(sys.argv) < 4:
            # todo - should throw an error
            print('ERRORR!!!!!!')

        ## Search by X review count
        searchReviewCount = sqlCtx.sql(" SELECT * FROM purchases WHERE reviews.avg_rating {} {} ".format(sys.argv[2], sys.argv[3]))
        for i in searchReviewCount.collect(): print ("Matched Product: " + i.Id + ", Rating avg: " + i.reviews.avg_rating + ", title: " + i.title)

    if sys.argv[1] == 'search-category':

        # Arguments
        ## 2 - query string

        if len(sys.argv) < 3:
            # todo - should throw an error
            print('ERRORR!!!!!!')

        ## Search by category, partial matching allowed, case insensitive
        searchTitles = sqlCtx.sql(""" SELECT * FROM purchases WHERE lower(categories) LIKE "%{}%" """.format(sys.argv[2]))
        for i in searchTitles.collect(): print("Matched Product: " + i.Id + ", title: " + i.title)

    if sys.argv[1] == 'search-customer-rating':

        # Arguments
        ## 2 - operator, eg. >, >=, =, <, <=
        ## 3 - avg rating

        if len(sys.argv) < 3:
            # todo - should throw an error
            print('ERRORR!!!!!!')

        ## Search by X review count
        # searchCustomerId = sqlCtx.sql(""" SELECT * FROM purchases WHERE reviews.reviews.cutomer = "{}" """.format(sys.argv[2]))

        searchCustomerId = sqlCtx.sql(" SELECT * FROM purchases WHERE reviews.rating > {} ".format(sys.argv[2]))
        for i in searchCustomerId.collect(): print(
            "Matched Product: " + i.Id + ", Customer: " + i.reviews.rating + ", title: " + i.title)

    if sys.argv[1] == 'search-customer':

        # Arguments
        ## 2 - operator, eg. >, >=, =, <, <=
        ## 3 - avg rating

        if len(sys.argv) < 3:
            # todo - should throw an error
            print('ERRORR!!!!!!')

        ## Search by X review count
        # searchCustomerId = sqlCtx.sql(""" SELECT * FROM purchases WHERE reviews.reviews.cutomer = "{}" """.format(sys.argv[2]))
        searchCustomerId = sqlCtx.sql(" SELECT * FROM purchases WHERE reviews.reviews.rating > {} ".format(sys.argv[2]))
        for i in searchCustomerId.collect(): print(
            "Matched Product: " + i.Id + ", Customer: " + i.reviews.reviews.cutomer + ", title: " + i.title)

    # todo - Best X sellers of a certain category
    # todo - The number of customers co-purchasing same product of a user.

except Exception as err:
    print(err)

