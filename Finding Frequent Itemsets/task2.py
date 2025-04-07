import sys
import time
import argparse
import json
from itertools import islice, combinations, groupby
import pyspark
from pyspark import SparkContext

# Function for phase 2 of the SON algorithm
def validateCandidates(iterator, flat_candidates):
    """
    Count occurrences of candidate itemsets in the data
    """
    for user, biz_set in iterator:
        basket_items = set(biz_set)
        for candidate in flat_candidates:
            if set(candidate).issubset(basket_items):
                yield (candidate, 1)

# Function for use in step 3 of aPriori algorithm
def getCandidatesForNextSize(freq, nextSize):
    """
    Generate candidate itemsets for the next iteration:
    1. Take frequent itemsets from the current iteration
    2. Join pairs of frequent itemsets to create larger candidates
    3. Apply the a-priori property to prune invalid candidates
    4. Return valid candidate itemsets of the specified size
    """
    candidates = set()
    freq_list = sorted(freq)
    for i in range(len(freq_list)):
        for j in range(i + 1, len(freq_list)):
            candidate = tuple(sorted(set(freq_list[i]).union(set(freq_list[j]))))
            if len(candidate) == nextSize:
                # Check that all (nextSize - 1) subsets are frequent
                all_subsets_frequent = True
                for subset in combinations(candidate, nextSize - 1):
                    if tuple(sorted(subset)) not in freq:
                        all_subsets_frequent = False
                        break
                if all_subsets_frequent:
                    candidates.add(candidate)
    return list(candidates)
    
def pcy(iterator, totalCount, support_threshold, hashSize):
   """
   Implement the PCY (Park-Chen-Yu) algorithm:
   1. Process baskets to calculate partition-specific threshold
   2. For singleton pass, hash item pairs into a hash table
   3. Use hash table to identify potentially frequent pairs
   4. Generate candidates with a-priori pruning and hash-based filtering
   5. Validate candidates against data in each iteration
   6. Continue until no new frequent itemsets are found
   7. Return all frequent itemsets discovered
   
   HINT: The Major bottleneck for this data is the processing of the size-2 candidates. It is recomended you use a hashtable instead of a dictionary to store their counts. 
   During step 2 you can also create size-2 combinations from each basket, and hash the pair by index = hash(pair)%hashTableSize, and increment the corresponding entry by 1  hashTable[index]+=1
   """
   baskets = list(iterator)
   partition_size = len(baskets)
   if partition_size == 0:
       return iter([])
   
   part_threshold = support_threshold * (partition_size / float(totalCount))
   singleton_counts = {}
   hashTable = [0] * hashSize
   for _, biz_set in baskets:
        biz_list = list(biz_set)
        # Count singletons
        for item in biz_list:
            key = (item,)
            singleton_counts[key] = singleton_counts.get(key, 0) + 1
        # Hash pairs
        for i in range(len(biz_list)):
            for j in range(i + 1, len(biz_list)):
                pair = tuple(sorted((biz_list[i], biz_list[j])))
                idx = hash(pair) % hashSize
                hashTable[idx] += 1
   freq_singletons = {
        item for item, count in singleton_counts.items() if count >= part_threshold
    }
   frequent_buckets = set()
   for idx, bucket_count in enumerate(hashTable):
        if bucket_count >= part_threshold:
            frequent_buckets.add(idx)
   freq_singletons_list = sorted(freq_singletons)
   candidate_pairs = []
   for i in range(len(freq_singletons_list)):
        for j in range(i + 1, len(freq_singletons_list)):
            pair = tuple(sorted((freq_singletons_list[i][0], freq_singletons_list[j][0])))
            # Check if the pair's bucket is frequent
            bucket_idx = hash(pair) % hashSize
            if bucket_idx in frequent_buckets:
                candidate_pairs.append(pair)
   pair_counts = {}
   for _, biz_set in baskets:
        basket_items = set(biz_set)
        for pair in candidate_pairs:
            if set(pair).issubset(basket_items):
                pair_counts[pair] = pair_counts.get(pair, 0) + 1

   freq_pairs = {p for p, c in pair_counts.items() if c >= part_threshold}
   result_itemsets = set(freq_singletons) | freq_pairs
   current_freq = freq_pairs
   k = 3
   while current_freq:
        next_candidates = getCandidatesForNextSize(current_freq, k)
        if not next_candidates:
            break

        # Count occurrences of each candidate
        candidate_counts = {}
        for _, biz_set in baskets:
            basket_items = set(biz_set)
            for cand in next_candidates:
                if set(cand).issubset(basket_items):
                    candidate_counts[cand] = candidate_counts.get(cand, 0) + 1
        current_freq = {cand for cand, cnt in candidate_counts.items() if cnt >= part_threshold}
        result_itemsets |= current_freq
        k += 1
   return iter(result_itemsets)



# Main function to orchestrate the workflow
def main(rdd, filter_threshold, support_threshold, outputJson, hashSize):
    """
    Main function controlling the workflow and implementing the SON algo:
    1. Preprocess data (remove header, ect)
    2. Build Case 1 market-basket model (user -> businesses)
    3. Filter users who reviewed more than filter_threshold businesses
    4. Apply the SON algorithm with pcy
    5. Output results
    """
    out = {}
    # Header Removed
    header = rdd.first()
    data = rdd.filter(lambda line: line != header)

    parsed_rdd = data.map(lambda line: line.split(','))
    user_baskets = parsed_rdd \
        .groupByKey() \
        .mapValues(lambda biz_list: set(biz_list))
    
    filtered_baskets = user_baskets.filter(lambda x: len(x[1]) > filter_threshold)
    basket_count = filtered_baskets.count()
    print("Filtered basket count:", basket_count)

    local_candidates = filtered_baskets \
        .mapPartitions(lambda partition: pcy(iterator=partition,
                                             totalCount=basket_count,
                                             support_threshold=support_threshold,
                                             hashSize=hashSize)) \
        .distinct() \
        .collect()
    local_candidates_sorted = sorted(local_candidates, key=lambda x: (len(x), x))
    print("Local Candidates:", local_candidates_sorted)

    sc = filtered_baskets.context
    broadcast_candidates = sc.broadcast(local_candidates_sorted)
    candidate_counts = filtered_baskets.mapPartitions(
        lambda partition: validateCandidates(partition, broadcast_candidates.value)
    )

    global_candidate_counts = candidate_counts.reduceByKey(lambda a, b: a + b)


    frequent_itemsets = global_candidate_counts.filter(lambda x: x[1] >= support_threshold).collect()

    frequent_itemsets_sorted = sorted(frequent_itemsets, key=lambda x: (len(x[0]), x[0]))
    print("Frequent Itemsets:", frequent_itemsets_sorted)

    formatted_frequent_itemsets = [
        {"itemset": list(candidate), "support": count}
        for (candidate, count) in frequent_itemsets_sorted
    ]

    # Prepare output
    #out["Candidates"] = local_candidates_sorted
    #out["Frequent Itemsets"] = formatted_frequent_itemsets

    candidates_grouped = []
    for size, group in groupby(local_candidates_sorted, key=len):
        group_list = []
        for candidate_tuple in group:
            group_list.append(list(candidate_tuple))  # tuple -> list
        candidates_grouped.append(group_list)

    freq_itemsets_grouped = []
    for size, group in groupby(frequent_itemsets_sorted, key=lambda x: len(x[0])):
        group_list = []
        for (candidate_tuple, support) in group:
            group_list.append({
                "itemset": list(candidate_tuple),
                "support": support
            })
        freq_itemsets_grouped.append(group_list)
    
    out['Candidates'] = candidates_grouped
    out['Frequent Itemsets'] = freq_itemsets_grouped

    time1 = time.time()
    out['Runtime'] = time1-time0
    with open(outputJson, 'w') as f:
        json.dump(out, f)
  
if __name__ == '__main__':
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Task 2: SON algorithm on Yelp data')
    parser.add_argument('--f', type=int, help='Filter threshold for users')
    parser.add_argument('--t', type=int, help='Support threshold for frequent itemsets')
    parser.add_argument('--input_file', type=str, help='Input file path')
    parser.add_argument('--output_file', type=str, help='Output file path')
    
    args = parser.parse_args()
    
    # Extract arguments
    filter_threshold = args.f
    support_threshold = args.t
    input_file = args.input_file
    output_file = args.output_file
    
    hashSize = 30000000
    
    # Record start time
    time0 = time.time()
    
    # Initialize Spark
    sc_conf = pyspark.SparkConf() \
                .setAppName('task2') \
                .setMaster('local[*]') \
                .set('spark.driver.memory', '12g') \
                .set('spark.executor.memory', '8g')
    
    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")
    
    # Read input file
    rdd = sc.textFile(input_file)
    
    # Run main function
    main(rdd, filter_threshold, support_threshold, output_file, hashSize)
    
    # Stop Spark context
    sc.stop()