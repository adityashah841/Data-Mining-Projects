# Data-Mining-Projects

This repository contains four distinct Spark projects, each addressing different data analysis challenges using distributed processing. Below is an overview of each project, outlining its purpose, key functionalities, and overall workflow.

---

## Project 1: Review Analysis and Category-Based Ratings

**Overview:**  
This project consists of two scripts that analyze review data to extract meaningful insights and perform text and rating analysis.

### Script 1: Review Analysis and Word Frequency Extraction
- **Purpose:**  
  Extract various metrics (e.g., count of distinct businesses, user review thresholds, reviews per year, and reviews with specific star ratings) and compute the most frequent words in reviews for a specific year.
- **Key Steps:**
  - Configures Spark context and parses command-line arguments.
  - Preprocesses review texts (lowercase, punctuation removal, stopword filtering).
  - Computes metrics such as distinct business count, user review counts, and yearly review counts.
  - Extracts top frequent words using a flatMap operation.
  - Outputs results in JSON format.

### Script 2: Category-Based Average Star Rating
- **Purpose:**  
  Join review data with business data to calculate average star ratings for business categories and extract the top categories based on these averages.
- **Key Steps:**
  - Loads review and business datasets, filtering and grouping data.
  - Joins datasets on business IDs to map reviews to business categories.
  - Computes the average star rating for each category.
  - Sorts and outputs the top N categories based on the average ratings.

---

## Project 2: SON Algorithm for Frequent Itemset Mining

**Overview:**  
This project implements the SON (Savasere, Omiecinski, and Navathe) algorithm using two different approaches for mining frequent itemsets in market-basket data.

### Task 1: SON with A-Priori
- **Purpose:**  
  Discover frequent itemsets using the SON algorithm combined with the A-Priori method.
- **Key Steps:**
  - Preprocesses input data by filtering records by year and grouping baskets (by user or business).
  - Runs the A-Priori algorithm locally on each data partition to generate candidate itemsets.
  - Broadcasts local candidates for global validation and counts occurrences across the full dataset.
  - Groups and outputs candidate and frequent itemsets with support counts.

### Task 2: SON with PCY
- **Purpose:**  
  Improve the efficiency of size-2 candidate generation by integrating the PCY (Park-Chen-Yu) algorithm into the SON framework.
- **Key Steps:**
  - Preprocesses and groups data to form user baskets.
  - Uses PCY to count singleton frequencies and hash item pairs into buckets.
  - Filters and generates candidate pairs based on hash table counts.
  - Validates candidate itemsets globally and outputs frequent itemsets grouped by size.

---

## Project 3: MinHash and Locality Sensitive Hashing (LSH) for Similarity Detection

**Overview:**  
This project implements MinHash and LSH techniques to identify similar businesses based on their user review sets.

- **Purpose:**  
  Generate candidate pairs of similar businesses by computing MinHash signatures and applying LSH to quickly narrow down the search space, followed by Jaccard similarity filtering.
- **Key Steps:**
  - **Data Preparation:**  
    Reads JSON records and groups them by business, forming baskets of user IDs.
  - **MinHash Signature Generation:**  
    Creates a set of hash functions and computes a MinHash signature for each business.
  - **LSH Banding:**  
    Splits each MinHash signature into multiple bands; businesses with identical band signatures are grouped together.
  - **Candidate Pair Generation:**  
    Forms candidate pairs from groups sharing the same band signature.
  - **Similarity Filtering:**  
    Computes Jaccard similarity for each candidate pair and retains only those exceeding a defined threshold.
  - **Output:**  
    Saves candidate pairs and the final similar pairs (with similarity scores) to output files and logs the runtime.

---

## Project 4: Item-Based Collaborative Filtering Recommender
**Overview:**  
An end-to-end item-based CF system on Yelp data. First **build** an item–item similarity model; then **predict** user ratings on held-out pairs.

**Key Steps:**

1. **Model Building (`build.py`)**  
   - Load training reviews and group by `user_id` → list of `(business, rating)`.  
   - For each user, emit all co-rated business pairs with their two ratings.  
   - Group by `(b1, b2)`, filter pairs with fewer than _m_ common raters.  
   - Compute Pearson correlation and record `num_common`.  
   - Write one JSON line per pair:
     ```json
     { "b1": "...", "b2": "...", "sim": 0.1234, "num_common": 42 }
     ```

2. **Rating Prediction (`predict.py`)**  
   - Rebuild in-memory maps:
     - `user_map`: user → `[(biz, stars), …]`  
     - `user_avg`: user mean ratings  
     - `biz_avg`: business mean ratings  
     - `global_avg`: overall mean rating  
   - Load item–item model into a dict `((b1,b2) → (sim, num_common))`.  
   - For each test `(user, item)`:
     - Retrieve all businesses `j` rated by `user`.  
     - Look up `(item,j)` or `(j,item)` → `(sim, cnt)`.  
     - Compute **shrinkage**: `shrink = cnt / (cnt + 50)`, `w = sim * shrink`.  
     - Keep only `w > 0`, sort by descending `w`, take top _n_ neighbors.  
     - Compute the centered weighted prediction:
       ```
       p_{u,i} = r̄_u
               + ( ∑_j w · (r_{u,j} − r̄_u) )
                 / ∑_j w
       ```
     - Fallback to `r̄_u` if no neighbors.  
   - Output one JSON line per prediction:
     ```json
     { "user_id": "USER", "business_id": "BIZ", "stars": 3.75 }
     ```
