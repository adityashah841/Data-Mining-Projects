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
    # Iterating over all the baskets in the data, it takes the second element of the bucket as the list of items in the bucket and converts it into a set
    # It them iterates over the globally broadcasted candidates and checks for all the candidates if they are a subset of the basket items
    # If it a part, it yields a tuple (candidate, 1) that is later reduced in the 2nd reduce phase of the SON algorithm for verificantion 
    for basket in iterator:
        basket_items = set(basket[1])
        for candidate in flat_candidates:
            if set(candidate).issubset(basket_items):
                yield (candidate, 1)

# Function for use in step 4 of aPriori algorithm
def getCandidatesForNextSize(freq, nextSize):
    """
    Generate candidate itemsets for the next iteration:
    1. Take frequent itemsets from the current iteration
    2. Join pairs of frequent itemsets to create larger candidates
    3. Apply the a-priori property to prune invalid candidates
    4. Return valid candidate itemsets of the specified size
    """
    # This function creates newer candidates based on the nextSize number. It initializes an empty set of candidates, iterates over all frequest items using i and j,
    # Then creates a candidate by taking a union of ith and jth item from the sorted frequent itemsets. and sorts it again.
    # Thn it checks the length of the candidate created and if it is equal to the nextSize, it checks if all of it's immediate subsets are in the frequent items or not
    # If they are, the all_subsets_frequent flag remains true and the candidate is added to the candidates set. Finally, this list of candidates is returned.
    # Debug
    #print("Obtaining next size candidate")
    candidates = set()
    freq_list = sorted(freq)
    for i in range(len(freq_list)):
        for j in range(i+1, len(freq_list)):
            candidate = tuple(sorted(set(freq_list[i]).union(set(freq_list[j]))))
            #Debug
            print(freq_list, candidate, len(candidate), nextSize)
            if len(candidate) == nextSize:
                all_subsets_frequent = True
                for subset in combinations(candidate, nextSize - 1):
                    if tuple(sorted(subset)) not in freq:
                        all_subsets_frequent = False
                        break
                if all_subsets_frequent:
                    candidates.add(candidate)
    #Debug
    #print("Candidates in getCandidatesForNextSize function: ",candidates)
    return list(candidates)

def aPriori(iterator, threshold, totalCount):
    """
    Implement the A-Priori algorithm:
    1. Process baskets to calculate partition-specific threshold
    2. Generate singleton itemsets from the data
    3. Identify frequent itemsets by counting and comparing to threshold
    4. Generate candidates for next iteration using frequent itemsets
    5. Validate each new set of candidates against the data
    6. Repeat until no new frequent itemsets are found
    7. Return the complete set of frequent itemsets
    """
    # This function runs the apriori algorithm locally on a partition of baskets. The iterator is converted to a list to compute length and iterate over it multiple times
    # The partition size is then calculated and a precautionary measure is taken to avoid 0 length partitions
    # The global threshold is then modified to the partition threshold for that particular partition.  
    baskets = list(iterator)
    partition_size = len(baskets)
    if partition_size == 0:
        return iter([])
    part_threshold = threshold * (partition_size / float(totalCount))
    # Debug
    #print("/////////////////////","threshold: ",threshold,"partition size: ", partition_size,"total count: ",totalCount,"partition threshold: ", part_threshold)
    
    # Now I start counting the singletons. Iterating over each bucket and each item in the bucket, the items are represented in tuples, making them consistent with the larger itemsets
    # then it justs looks up the dictionary and updates the count and they are filtered over the partition threshold and stored in result as a list
    singleton_counts = {}
    for basket in baskets:
        for item in basket[1]:
            key = (item,)
            singleton_counts[key] = singleton_counts.get(key, 0) + 1
    #print("Frequent singletons in partition:", singleton_counts)
    freq_itemsets = {item for item, count in singleton_counts.items() if count >= part_threshold}
    result = list(freq_itemsets)
    # Here we start iterating over the nextSize of the candidates. All candidates are then checked for the partition threshold and if they pass, they are extended onto the results
    k = 2
    while freq_itemsets:
        candidates = getCandidatesForNextSize(freq_itemsets, k)
        # Debug
        #print("Returned Candidates: ", candidates)
        candidate_counts = {}
        for basket in baskets:
            basket_items = set(basket[1])
            for candidate in candidates:
                if set(candidate).issubset(basket_items):
                    candidate_counts[candidate] = candidate_counts.get(candidate, 0) + 1
        #Debug
        #print("Count for candidates: ", candidate_counts)
        freq_itemsets = {candidate for candidate, count in candidate_counts.items() if count >= part_threshold}
        result.extend(list(freq_itemsets))
        k += 1
    return iter(result)

def main(rdd, case, threshold, outputJson, year_filter, hashSize=None):
    """
    Main function controlling the workflow and implementing the SON algo:
    1. Preprocess data (remove header, filter by year, ect)
    2. Convert data into baskets based on case
    3. Apply the SON algorithm with A-Priori
    4. Output results
    """
    out = {}
    # Removing header of the csv
    header = rdd.first()
    rdd_no_header = rdd.filter(lambda line: line != header)

    # Parsing and filtering each line on the year
    def parse_n_filter(line):
         parts = line.split(',')
         rec_year=int(parts[0].strip().strip('"'))
         if rec_year==year_filter:
              return (rec_year, parts[1].strip(), parts[2].strip())
         else:
              return None
    filtered_rdd = rdd_no_header.map(parse_n_filter).filter(lambda x: x is not None)
    # Debug remove before submitting
    #print("Sample filtered records:", filtered_rdd.take(5))

    # If else case statement. Used to group data differently
    if case==1:
         baskets = filtered_rdd.map(lambda x: (x[1], x[2])) \
                            .distinct() \
                            .groupByKey() \
                            .mapValues(lambda businesses: sorted(set(businesses)))
    else:
         baskets = filtered_rdd.map(lambda x: (x[2], x[1])) \
                            .distinct() \
                            .groupByKey() \
                            .mapValues(lambda users: sorted(set(users)))
    # Debug, take out before submitting
    #print("Sample baskets:", baskets.take(5))
    # Counting baskets
    total_count = baskets.count()
    # Debug
    #print("Total basket count:", total_count)
    # Using the apriori with the mapPartitions for SON Phase 1
    local_candidates = baskets.mapPartitions(lambda partition: aPriori(partition, threshold, total_count)) \
                                .distinct() \
                                .collect()
    local_candidates_sorted = sorted(local_candidates, key=lambda x: (len(x), x))
    # Debug
    #print("Local Candidates:", local_candidates_sorted)

    # SON phase 2 starts from here. Results of local candidates sorted is broadcasted to all nodes in the cluster. This is folllowed by validation of candidates.
    broadcast_candidates = sc.broadcast(local_candidates_sorted)
    candidate_counts = baskets.mapPartitions(lambda partition: validateCandidates(partition, broadcast_candidates.value))
    global_candidate_counts = candidate_counts.reduceByKey(lambda a, b: a + b)
    # Debug
    #print("Global candidate counts:", global_candidate_counts.collect())
    
    frequent_itemsets = global_candidate_counts.filter(lambda x: x[1] >= threshold).collect()
    frequent_itemsets_sorted = sorted(frequent_itemsets, key=lambda x: (len(x[0]), x[0]))
    # Debug
    #print("Frequent Itemsets:", frequent_itemsets_sorted)

    formatted_frequent_itemsets = [
    {"itemset": list(candidate), "support": count}
    for (candidate, count) in frequent_itemsets_sorted
    ]
    #print("Frequent Itemsets Formatted:",formatted_frequent_itemsets)
    #out['Candidates'] = local_candidates_sorted
    #out['Frequent Itemsets'] = formatted_frequent_itemsets
    candidates_grouped = []
    for size, group in groupby(local_candidates_sorted, key=len):
        group_list = []
        for candidate_tuple in group:
            group_list.append(list(candidate_tuple))
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
	parser = argparse.ArgumentParser(description='HW2T1')	
	parser.add_argument('--y', type=int, default=2017, help ='Filter year')
	parser.add_argument('--c', type=int, default=1, help ='case number')
	parser.add_argument('--t', type=int, default=10, help ='frequent threshold')
	parser.add_argument('--input_file', type=str, default='../data/small2.csv', help ='input file')
	parser.add_argument('--output_file', type=str, default='./HW2task1.json', help ='output  file')

	args = parser.parse_args()

	case = args.c
	threshold = args.t
	inputJson = args.input_file
	outputJson = args.output_file
	year_filter = args.y

	time0 = time.time()

	# Read Input
	sc = SparkContext()
	sc.setLogLevel("ERROR")
	rdd = sc.textFile(inputJson)

	main(rdd, case, threshold, outputJson, year_filter)
	
	sc.stop()