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

        # Search by category, partial matching allowed, case insensitive
        # searchTitles = sqlCtx.sql(""" SELECT * FROM purchases WHERE lower(categories) LIKE "%{}%" """.format(sys.argv[2]))
        # for i in searchTitles.collect(): print("Matched Product: " + i.Id + ", title: " + i.title)
        searchTitles = amzDF.withColumn("categories", explode("categories")) \
            .select(col("categories.*"), "Id", "title") \
            .where(""" lower(name) LIKE "%{}%" """.format(sys.argv[2].lower())) \
            .distinct()

        rowCount = 0
        for i in searchTitles.collect():
            rowCount += 1
            print(str(rowCount) + " - Matched Product: " + i.Id + ", title: " + i.title + ", Category: " + i.name)

    if sys.argv[1] == 'search-customer-rating':

        # Arguments
        ## 2 - operator, eg. >, >=, =, <, <=
        ## 3 - avg rating

        if len(sys.argv) < 4:
            # todo - should throw an error
            print('ERRORR!!!!!!')

        searchCustomerId = amzDF.withColumn("reviews", explode("reviews")).select("*", col("reviews.*")) \
            .where(""" rating{}{} """.format(sys.argv[2], sys.argv[3]))

        for i in searchCustomerId.collect(): print("Matched Product: " + i.Id + ", Customer: " + i.cutomer + ", Rating: " + i.rating + ", title: " + i.title)

    if sys.argv[1] == 'search-customer':

        # Arguments
        ## 2 - operator, eg. >, >=, =, <, <=
        ## 3 - avg rating

        if len(sys.argv) < 3:
            # todo - should throw an error
            print('ERRORR!!!!!!')

        searchCustomerId = amzDF.withColumn("reviews", explode("reviews")).select("*", col("reviews.*"))\
            .where(""" cutomer="{}" """.format(sys.argv[2]))

        for i in searchCustomerId.collect(): print("Matched Product: " + i.Id + ", Customer: " + i.cutomer + ", title: " + i.title)

    if sys.argv[1] == 'recommend-customer':

        # Arguments
        ## 2 - customer ID#

        if len(sys.argv) < 3:
            # todo - should throw an error
            print('ERRORR!!!!!!')

        # searchCustomerId = amzDF.withColumn("reviews", explode("reviews")) \
        #     .withColumn("categories", explode("categories")) \
        #     .select(col("reviews.*"), col("categories"), "title") \
        #     .where(""" cutomer="{}" """.format(sys.argv[2]))
        # searchCustomerId.printSchema()
        # searchCustomerId.show()

        searchCustomerReviewsById = amzDF.withColumn("reviews", explode("reviews")) \
            .select(col("reviews.*"), "ASIN", "categories", "title", "similar") \
            .where(""" cutomer="{}" """.format(sys.argv[2]))
        # searchCustomerReviewsById.printSchema()
        # searchCustomerReviewsById.show()

        recommendableCategories = []
        recommendableCategoriesProductsTracker = []
        similarProductsTracker = []
        recommendableCategoriesProducts = []
        similarProducts = []

        print("Because you have favorably reviewed:")
        for i in searchCustomerReviewsById.collect():
            rowCount += 1
            # print(str(rowCount) + " Matched Customer: " + i.cutomer + ", rating: " + i.rating + ", title: " + i.title + " # of Categories for Product: " + str(len(i.categories)))

            if int(i.rating) > 3:
                print("-- Title: " + i.title + "(" + i.ASIN + ") - Rating: " + i.rating)

                categoryCount = 0
                categoriesLength = len(i.categories)
                for category in i.categories:
                    categoryCount += 1
                    # print("++Category: " + category)
                    if category not in recommendableCategories:
                        if categoriesLength == categoryCount: # only adds the last category, todo - not good logic that represents the "Most" important categories
                            recommendableCategories.append(category.id)

                for similarProduct in i.similar:
                    # print("++Category: " + category)
                    if similarProduct not in similarProductsTracker:
                        similarProductsTracker.append(similarProduct)

        #
        # Other products, related category
        #
        recommendableCategoriesString = ''

        # print('Recommendable categories: ')
        for category in recommendableCategories:
            # print("-- " + category)
            recommendableCategoriesString += '\'' + category + '\','

        recommendableCategoriesString = recommendableCategoriesString.rstrip(',')

        # print(recommendableCategoriesString)

        recommendedProductsByCategory = amzDF.withColumn("categories", explode("categories")) \
            .select(col("categories.*"), "ASIN", "title") \
            .where(""" id IN ({}) """.format(recommendableCategoriesString))
        # recommendedProductsByCategory.printSchema()
        # recommendedProductsByCategory.show()

        rowCount = 0
        for product in recommendedProductsByCategory.collect():
            if product.ASIN not in recommendableCategoriesProductsTracker:
                recommendableCategoriesProducts.append(product)
                recommendableCategoriesProductsTracker.append(product.ASIN)
                rowCount += 1
                # print(str(rowCount) + " | Title: " + product.title + "(" + product.ASIN + ")")

        print("Product recommendations based on your favorable reviews found: " + str(len(recommendableCategoriesProducts)))
        #
        # Show top 5 recommended products
        #
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
            # print("-- " + product)
            similarProductsString += '\'' + product + '\','

        similarProductsString = similarProductsString.rstrip(',')

        recommendedProductsBySimilarity = amzDF.select("ASIN", "title") \
            .where(""" ASIN IN ({}) """.format(similarProductsString))
        # recommendedProducts.printSchema()
        # recommendedProducts.show()

        rowCount = 0
        # print("Similar items you might like!")
        for product in recommendedProductsBySimilarity.collect():
            rowCount += 1
            similarProducts.append(product)
            # print(str(rowCount) + " | Title: " + product.title + "(" + product.ASIN + ")")

        print('Similar products found: ' + str(len(similarProducts)))
        # Show top 5 similar products
        for product in random.sample(similarProducts, 5):
            print("-- Title: " + product.title + "(" + product.ASIN + ")")
        #
        # / Similar Products
        #

    # todo - Best X sellers of a certain category
    # todo - The number of customers co-purchasing same product of a user.

except Exception as err:
    print(err)




# A1GIL64QK68WKL - 17 hits