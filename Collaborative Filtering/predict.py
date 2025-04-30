import argparse
import json
import time
import pyspark






def compute_weighted_avg(target_user,
                         target_item,
                         target_user_rated_items,
                         model,
                         user_avg,
                         biz_avg,
                         global_avg,
                         n_weights):
    """
    Item-based CF prediction for (target_user, target_item).

    1. Get the user’s mean rating ru (fallback to global_avg if user unseen).
    2. For each business j the user has rated:
       - Look up similarity sim = model[(target_item, j)] or model[(j, target_item)].
       - Compute a shrinkage factor: shrink = cnt / (cnt + 50).
       - w = sim * shrink; keep only w > 0.
       - neighbor contribution = (w, (r_uj - ru)).
    3. If no positive-weight neighbors, return ru.
    4. Otherwise take up to n_weights highest-w neighbors,
       compute:
           diff = sum(w * (r_uj - ru)) / sum(w)
           return ru + diff.
    """
    # 1) user mean
    ru = user_avg.get(target_user, global_avg)

    # 2) collect positive-weight neighbors
    neigh = []
    for j, ruj in target_user_rated_items:
        # look up sim & count
        if (target_item, j) in model:
            sim, cnt = model[(target_item, j)]
        elif (j, target_item) in model:
            sim, cnt = model[(j, target_item)]
        else:
            continue

        # shrinkage
        shrink = cnt / float(cnt + 50)
        w = sim * shrink
        if w > 0:
            neigh.append((w, ruj - ru))

    # 3) no neighbors → fallback to user mean
    if not neigh:
        return ru

    # 4) select top-n by descending w
    neigh.sort(key=lambda x: x[0], reverse=True)
    top_n = neigh[:n_weights]

    denom = sum(w for w, _ in top_n)
    if denom == 0:
        return ru

    diff = sum(w * delta for w, delta in top_n) / denom
    return ru + diff



def main(train_file, test_file, model_file, out_file, n, sc):
    # load train
    train = sc.textFile(train_file).map(json.loads)

    # user → [(biz, r)]
    user_map = (
        train
        .map(lambda r: (r['user_id'], (r['business_id'], r['stars'])))
        .groupByKey()
        .mapValues(list)
        .collectAsMap()
    )

    # user → ru_bar
    user_avg = {
        u: sum(r for _, r in items) / len(items)
        for u, items in user_map.items()
    }

    # biz → avg
    biz_avg = (
        train
        .map(lambda r: (r['business_id'], r['stars']))
        .groupByKey()
        .mapValues(lambda rs: sum(rs) / len(rs))
        .collectAsMap()
    )

    # global avg
    all_r = train.map(lambda r: r['stars'])
    tot, cnt = all_r.reduce(lambda a, b: a + b), all_r.count()
    global_avg = tot / cnt if cnt else 0.0

    # load model into dict
    raw = sc.textFile(model_file).map(json.loads).collect()
    model = {}
    for m in raw:
        model[(m['b1'], m['b2'])] = (m['sim'], m['num_co_rated'])

    # load test pairs
    tests = (
        sc.textFile(test_file)
          .map(json.loads)
          .map(lambda r: (r['user_id'], r['business_id']))
          .collect()
    )

    # predict & write
    with open(out_file, 'w') as f:
        for u, b in tests:
            rated = user_map.get(u, [])
            p = compute_weighted_avg(u, b, rated,
                                     model, user_avg,
                                     biz_avg, global_avg,
                                     n)
            f.write(json.dumps({
                'user_id':     u,
                'business_id': b,
                'stars':       p
            }) + '\n')




if __name__ == '__main__':
    start_time = time.time()
    sc_conf = pyspark.SparkConf() \
        .setAppName('hw4_predict') \
        .setMaster('local[*]') \
        .set('spark.driver.memory', '4g') \
        .set('spark.executor.memory', '4g')
    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")


    parser = argparse.ArgumentParser(description='A1T1')
    parser.add_argument('--train_file', type=str, default='./data/train_review.json')
    parser.add_argument('--test_file', type=str, default='./data/test_review.json')
    parser.add_argument('--model_file', type=str, default='./outputs/ HW4.model')
    parser.add_argument('--output_file', type=str, default='./outputs/HW4.val.out')
    parser.add_argument('--time_file', type=str, default='./outputs/HW4.time')
    parser.add_argument('--n', type=int, default=3)
    args = parser.parse_args()


    main(args.train_file, args.test_file, args.model_file, args.output_file, args.n, sc)
    sc.stop()


    # log time
    with open(args.time_file, 'w') as outfile:
        json.dump({'time': time.time() - start_time}, outfile)
    print('The run time is: ', (time.time() - start_time))