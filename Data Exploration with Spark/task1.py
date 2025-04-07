import pyspark
import json
import argparse
import re
from datetime import datetime

if __name__ == '__main__':
    sc_conf = pyspark.SparkConf() \
                     .setAppName('task1') \
                     .setMaster('local[*]') \
                     .set('spark.driver.memory', '8g') \
                     .set('spark.executor.memory', '4g')

    sc = pyspark.SparkContext(conf = sc_conf)
    sc.setLogLevel('OFF')

    parser = argparse.ArgumentParser(description='A1T1')
    parser.add_argument('--input_file', type=str, default = '../data/review.json', help ='input file')
    parser.add_argument('--output_file', type=str, default = './hw1t1.json', help = 'output file')
    parser.add_argument('--stopwords', type=str, default = '../data/stopwords', help = 'stopwords file')
    parser.add_argument('--m', type=int, default = '10', help = 'review threshold m')
    parser.add_argument('--s', type=int, default = '2', help = 'star rating')
    parser.add_argument('--i', type=int, default = '10', help = 'top i frequent words')
    parser.add_argument('--y', type=int, default = '2018', help = 'year')
    args = parser.parse_args()

    '''
    YOUR CODE HERE
    '''
    def is_review_in_year(review, year):
        review_year = extract_year(review['date'])
        if review_year is None:  
            return False
        return int(review_year) == int(year)

    def text_preprocessing(txt, stopwords):
        txt = txt.lower()

        words = txt.split(" ")
        words = [wrd.strip("()[],.!?:;") for wrd in words]
        # for word in re.findall(r"\b[a-zA-Z0-9&':]+\b", txt):
        #     if word and word not in stopwords:
        #         words.append(word)
        # # print(words)
        return [wrd for wrd in words if wrd!='' and wrd not in stopwords]
    
    def extract_year(date_string):
        try:
            date_object = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
            return str(date_object.year)
        except ValueError:
            return None
    
    reviews_rdd = sc.textFile(args.input_file).map(json.loads)
    stopwords = set(sc.textFile(args.stopwords).collect())

    # A
    distinct_bus = reviews_rdd.map(lambda r: r['business_id']).distinct().count()

    # B
    user_review_count = reviews_rdd.map(lambda r: (r['user_id'], 1)).reduceByKey(lambda x, y: x + y)
    users_more_than_m = user_review_count.filter(lambda x: x[1] > args.m).count()

    # C
    review_counts_per_year = (
        reviews_rdd.map(lambda r: (extract_year(r['date']), 1)) \
           .filter(lambda x: x[0] is not None) \
           .reduceByKey(lambda x, y: x + y) \
           .collectAsMap()
    )

    # D
    num_reviews_with_stars = reviews_rdd.filter(lambda r: r['stars'] == args.s).count()


    # Debugging code .....************

    filtered_rdd = reviews_rdd.filter(lambda r: is_review_in_year(r, args.y))
    filtered_count = filtered_rdd.count()
    print(f"elements after filter: {filtered_count}")

    flatmapped_rdd = filtered_rdd.flatMap(lambda r: text_preprocessing(r['text'], stopwords))
    flatmapped_count = flatmapped_rdd.count()
    print(f"elements after flatMap: {flatmapped_count}")



    #.......***********

    # E
    top_words = (
        reviews_rdd.filter(lambda r: is_review_in_year(r, args.y))
        .flatMap(lambda r: text_preprocessing(r['text'], stopwords))
        .map(lambda word: (word, 1))
        .reduceByKey(lambda x, y: x + y)
        .takeOrdered(args.i, key=lambda x: (-x[1], x[0]))
    )


    output_data = {
        "A": distinct_bus,
        "B": users_more_than_m,
        "C": sorted([[int(year), count] for year, count in review_counts_per_year.items()], key=lambda x: (-x[1], -x[0])),  # Sorting
        "D": num_reviews_with_stars,
        "E": [word for word, _ in top_words]
    }
    print(top_words)
    with open(args.output_file, 'w') as f:
        json.dump(output_data, f, indent=4)

    sc.stop()