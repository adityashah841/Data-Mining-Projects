import argparse
import json
import time
import pyspark
import itertools
import math


def pearson_correlation(star_list1, star_list2):
    """
    Calculate the Pearson correlation coefficient between two lists of co-rated items.
    Returns 0 if either list has zero variance.
    """
    n = len(star_list1)
    if n == 0:
        return 0.0
    μ1 = sum(star_list1) / n
    μ2 = sum(star_list2) / n

    num = sum((x - μ1) * (y - μ2) for x, y in zip(star_list1, star_list2))
    den1 = math.sqrt(sum((x - μ1) ** 2 for x in star_list1))
    den2 = math.sqrt(sum((y - μ2) ** 2 for y in star_list2))
    if den1 == 0 or den2 == 0:
        return 0.0
    return round(num / (den1 * den2), 4)

def main(train_file, model_file, co_rated_thr, sc):
    reviews = sc.textFile(train_file).map(json.loads)

    # user → list of (biz, rating)
    user_items = (
        reviews
        .map(lambda r: (r['user_id'], (r['business_id'], r['stars'])))
        .groupByKey()
        .mapValues(list)
    )

    # emit every co-rated biz pair per user
    def to_pairs(u_and_items):
        _, items = u_and_items
        for (b1, r1), (b2, r2) in itertools.combinations(items, 2):
            if b1 < b2:
                yield ((b1, b2), (r1, r2))
            else:
                yield ((b2, b1), (r2, r1))

    pairs = user_items.flatMap(to_pairs)

    # group by biz-pair, filter by threshold
    co_rated = (
        pairs
        .groupByKey()
        .mapValues(list)
        .filter(lambda kv: len(kv[1]) >= co_rated_thr)
    )

    # compute similarity and num_common
    sims = co_rated.map(lambda kv: {
        'b1':          kv[0][0],
        'b2':          kv[0][1],
        'sim':         pearson_correlation(
                            [r for r, _ in kv[1]],
                            [s for _, s in kv[1]]),
        'num_co_rated':  len(kv[1])
    })

    # collect & write ONE file
    lines = sims.map(json.dumps).collect()
    with open(model_file, 'w') as f:
        for L in lines:
            f.write(L + '\n')


if __name__ == '__main__':
    start_time = time.time()
    sc_conf = pyspark.SparkConf() \
        .setAppName('hw4_build') \
        .setMaster('local[*]') \
        .set('spark.driver.memory', '4g') \
        .set('spark.executor.memory', '4g')
    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")

    parser = argparse.ArgumentParser(description='A1T1')
    parser.add_argument('--train_file', type=str, default='./data/train_review.json')
    parser.add_argument('--model_file', type=str, default='./outputs/HW4.model')
    parser.add_argument('--time_file', type=str, default='./outputs/HW4.time')
    parser.add_argument('--m', type=int, default=3)
    args = parser.parse_args()

    main(args.train_file, args.model_file, args.m, sc)
    sc.stop()

    # log time
    with open(args.time_file, 'w') as outfile:
        json.dump({'time': time.time() - start_time}, outfile)
    print('The run time is: ', (time.time() - start_time))
