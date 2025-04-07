import pyspark
import json
import argparse

if __name__ == '__main__':
    sc_conf = pyspark.SparkConf() \
                     .setAppName('task2') \
                     .setMaster('local[*]') \
                     .set('spark.driver.memory', '8g') \
                     .set('spark.executor.memory', '4g')

    sc = pyspark.SparkContext(conf = sc_conf)
    sc.setLogLevel('OFF')

    parser = argparse.ArgumentParser(description='A1T2')
    parser.add_argument('--review_file', type=str, default = '../data/review.json', help ='input review file')
    parser.add_argument('--business_file', type=str, default = '../data/business.json', help = 'input business file')
    parser.add_argument('--output_file', type=str, default = 'hw1t2.json', help = 'outputfile')
    parser.add_argument('--n', type=int, default = '50', help = 'top n categories with highest average stars')
    args = parser.parse_args()

    '''
    YOUR CODE HERE
    '''
    business_rdd = sc.textFile(args.business_file) \
        .map(lambda x: json.loads(x)) \
        .filter(lambda x: x.get("categories") is not None) \
        .flatMap(lambda x: [(x["business_id"], category.strip()) for category in x["categories"].split(", ")]) # Creates multiple entries of 1 business ID for each category it caters to multiple categories

    # Review dataset loading only business_id for join and the stars data
    review_rdd = sc.textFile(args.review_file) \
        .map(lambda x: json.loads(x)) \
        .map(lambda x: (x["business_id"], (x["stars"], x['text'])))
    
    review_rdd = review_rdd.distinct()
    review_rdd = review_rdd.map(lambda x: (x[0], x[1][0]))

    # Joining the Review and Business Data on business_id
    joined_rdd = review_rdd.join(business_rdd)  # (business_id, (stars, category))

    # Map (category, stars)
    category_rdd = joined_rdd.map(lambda x: (x[1][1], (x[1][0], 1)))  # (category, (stars, 1))
    category_avg_rdd = category_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda x: x[0] / x[1])  # initially (categories, (sum of stars, No. reviews for that business category)) and then (category, avg_stars)

    # Sort by avg_stars descending, then category alphabetically
    sorted_rdd = category_avg_rdd.sortBy(lambda x: (-x[1], x[0]))

    # Top N Categories
    top_n_categories = sorted_rdd.take(args.n)

    result = {"result": top_n_categories}
    
    with open(args.output_file, "w") as f:
        json.dump(result, f)

    sc.stop()




    