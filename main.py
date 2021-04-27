import sys
import random
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import col, explode

# https://datascience.stackexchange.com/questions/8549/how-do-i-set-get-heap-size-for-spark-via-python-notebook
conf = SparkConf().setAppName("Big Data App")
conf = (conf.setMaster('local[*]')
        .set('spark.executor.memory', '500M')
        .set('spark.driver.memory', '4G')
        .set('spark.driver.maxResultSize', '2G'))
sc = SparkContext(conf=conf)

# sc = SparkContext("local", "First App")

sqlCtx = SQLContext(sc)
amzDF = sqlCtx.read.json("data/amazon-meta.small-1000000.json")
# amzDF = sqlCtx.read.json("data/amazon-meta.small.json")
# amzDF = sqlCtx.read.json("data/amazon-meta.json")
amzDF.registerTempTable("purchases")

# print('---- Schema')
# amzDF.printSchema()
# print('-/-- Schema')

rowCount = 0

# Arguments
## 1 - search program

try:

    if len(sys.argv) <= 1:
        raise Exception ('Argument 1, utility name, must be specified')

    if sys.argv[1] == 'search-title': # python main.py search-title "harry potter"

        # Arguments
        ## 2 - query string

        if len(sys.argv) < 3:
            # todo - should throw an error
            print('ERRORR!!!!!!')

        ## Search by title, partial matching allowed, case insensitive
        searchTitles = sqlCtx.sql(""" SELECT ASIN, title FROM purchases WHERE lower(title) LIKE "%{}%" """.format(sys.argv[2])) \
            .distinct()

        for i in searchTitles.collect():
            rowCount += 1
            print(str(rowCount) + " - Matched Product: " + i.ASIN + ", title: " + i.title)


    if sys.argv[1] == 'search-review-count': # python main.py search-review-count ">" 15

        # Arguments
        ## 2 - operator, eg. >, >=, =, <, <=
        ## 3 - count

        if len(sys.argv) < 4:
            # todo - should throw an error
            print('ERRORR!!!!!!')

        ## Search by X review count
        searchReviewCount = sqlCtx.sql(" SELECT * FROM purchases WHERE reviews_summary.total {} {} ".format(sys.argv[2], sys.argv[3]))
        for i in searchReviewCount.collect():
            rowCount += 1
            print (str(rowCount) + " - Matched Product: " + i.ASIN + ", Reviews: " + i.reviews_summary.total + ", title: " + i.title)

    if sys.argv[1] == 'search-review-avg-rating': # python main.py search-review-avg-rating ">" 3

        # Arguments
        ## 2 - operator, eg. >, >=, =, <, <=
        ## 3 - avg rating

        if len(sys.argv) < 4:
            # todo - should throw an error
            print('ERRORR!!!!!!')

        ## Search by X review count
        searchReviewCount = sqlCtx.sql(" SELECT ASIN, reviews_summary, title  FROM purchases WHERE reviews_summary.avg_rating {} {} ".format(sys.argv[2], sys.argv[3])) \
            .distinct()

        for i in searchReviewCount.collect():
            rowCount += 1
            print (str(rowCount) + " - Matched Product: " + i.ASIN + ", Rating avg: " + i.reviews_summary.avg_rating + ", title: " + i.title)

    if sys.argv[1] == 'search-category': # python main.py search-category hebrew

        # Arguments
        ## 2 - query string

        if len(sys.argv) < 3:
            # todo - should throw an error
            print('ERRORR!!!!!!')

        # Search by category, partial matching allowed, case insensitive
        searchTitles = amzDF.withColumn("categories", explode("categories")) \
            .select(col("categories.*"), "Id", "title") \
            .where(""" lower(name) LIKE "%{}%" """.format(sys.argv[2].lower())) \
            .distinct()

        rowCount = 0
        for i in searchTitles.collect():
            rowCount += 1
            print(str(rowCount) + " - Matched Product: " + i.Id + ", title: " + i.title + ", Category: " + i.name)

    if sys.argv[1] == 'search-customer-rating': # python main.py search-customer-rating ">" 3

        # Arguments
        ## 2 - operator, eg. >, >=, =, <, <=
        ## 3 - rating

        if len(sys.argv) < 4:
            # todo - should throw an error
            print('ERRORR!!!!!!')

        searchCustomerId = amzDF.withColumn("reviews", explode("reviews")) \
            .select("title", "ASIN", col("reviews.*")) \
            .where(""" rating{}{} """.format(sys.argv[2], sys.argv[3])) \
            .distinct()

        for i in searchCustomerId.collect():
            rowCount += 1
            print(str(rowCount) + " - Matched Product: " + i.ASIN + ", Customer: " + i.cutomer + ", Rating: " + i.rating + ", title: " + i.title)

    if sys.argv[1] == 'search-customer': # python main.py search-customer A1GIL64QK68WKL

        # Arguments
        ## 2 - customer ID#

        if len(sys.argv) < 3:
            # todo - should throw an error
            print('ERRORR!!!!!!')

        searchCustomerId = amzDF.withColumn("reviews", explode("reviews")).select("*", col("reviews.*"))\
            .where(""" cutomer="{}" """.format(sys.argv[2]))

        for i in searchCustomerId.collect():
            rowCount += 1
            print(str(rowCount) + " - Matched Product: " + i.Id + ", Customer: " + i.cutomer + ", title: " + i.title)

    if sys.argv[1] == 'recommend-customer': # python main.py recommend-customer A1GIL64QK68WKL

        # Arguments
        ## 2 - customer ID#

        if len(sys.argv) < 3:
            # todo - should throw an error
            print('ERRORR!!!!!!')

        recommendableCategories = []
        recommendableCategoriesProductsTracker = []
        similarProductsTracker = []
        recommendableCategoriesProducts = []
        similarProducts = []

        searchCustomerReviewsById = amzDF.withColumn("reviews", explode("reviews")) \
            .select(col("reviews.*"), "ASIN", "categories", "title", "similar") \
            .where(""" cutomer="{}" """.format(sys.argv[2]))

        print("Because you have favorably reviewed:")
        for i in searchCustomerReviewsById.collect():
            rowCount += 1

            if int(i.rating) > 3:
                print("-- Title: " + i.title + "(" + i.ASIN + ") - Rating: " + i.rating)

                categoryCount = 0
                categoriesLength = len(i.categories)

                for category in i.categories:
                    categoryCount += 1
                    if category not in recommendableCategories:
                        if categoriesLength == categoryCount: # only adds the last category, # todo - not good logic to determine what the "Most" important categories are
                            recommendableCategories.append(category.id)

                for similarProduct in i.similar:
                    if similarProduct not in similarProductsTracker:
                        similarProductsTracker.append(similarProduct)

        #
        # Other products, related category
        #
        recommendableCategoriesString = ''

        for category in recommendableCategories:
            recommendableCategoriesString += '\'' + category + '\','

        recommendableCategoriesString = recommendableCategoriesString.rstrip(',')

        recommendedProductsByCategory = amzDF.withColumn("categories", explode("categories")) \
            .select(col("categories.*"), "ASIN", "title") \
            .where(""" id IN ({}) """.format(recommendableCategoriesString))

        rowCount = 0
        for product in recommendedProductsByCategory.collect():
            if product.ASIN not in recommendableCategoriesProductsTracker:
                recommendableCategoriesProducts.append(product)
                recommendableCategoriesProductsTracker.append(product.ASIN)
                rowCount += 1

        # Show top 5 recommended products
        print("Product recommendations based on your favorable reviews found: " + str(len(recommendableCategoriesProducts)))
        for product in random.sample(recommendableCategoriesProducts, 5):
            print("-- Title: " + product.title + "(" + product.ASIN + ")")

        #
        # / Other products, related category
        #

        #
        # Similar Products
        #
        similarProductsString = ''

        for product in similarProductsTracker:
            similarProductsString += '\'' + product + '\','

        similarProductsString = similarProductsString.rstrip(',')

        recommendedProductsBySimilarity = amzDF.select("ASIN", "title") \
            .where(""" ASIN IN ({}) """.format(similarProductsString))

        rowCount = 0
        for product in recommendedProductsBySimilarity.collect():
            rowCount += 1
            similarProducts.append(product)

        # Show top 5 similar products
        print('Similar products found: ' + str(len(similarProducts)))
        for product in random.sample(similarProducts, 5):
            print("-- Title: " + product.title + "(" + product.ASIN + ")")

        #
        # / Similar Products
        #

except Exception as err:
    print(err)