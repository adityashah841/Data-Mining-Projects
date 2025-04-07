
# README

This task implements a MinHash and Locality Sensitive Hashing (LSH) approach to identify similar businesses based on user reviews. The goal is to generate candidate pairs of businesses with high similarity using MinHash signatures and LSH, then filter these candidate pairs by computing their actual Jaccard similarity.

---

## Overview

The program takes an input file containing JSON records (each representing a review with at least a `business_id` and a `user_id`) and performs the following major steps:

1. **Preprocessing and Data Structuring:**
   - Reads and parses the input JSON records.
   - Groups records by business, forming baskets where each basket is a set of users who reviewed that business.

2. **MinHash Signature Generation:**
   - Generates a specified number of hash functions.
   - Computes a MinHash signature for each business based on its user set.

3. **Locality Sensitive Hashing (LSH):**
   - Splits the full MinHash signature into a fixed number of bands (each containing a number of rows).
   - Businesses that share the same band signature in at least one band are grouped together.
   - Candidate pairs are generated from these groups.

4. **Candidate Pair Filtering:**
   - The candidate pairs are saved to a candidate file.
   - For each candidate pair, the Jaccard similarity of the corresponding user sets is computed.
   - Only pairs with a Jaccard similarity greater than the specified threshold are retained.

5. **Output:**
   - The final similar business pairs (with their similarity scores) are written to an output file.
   - The script also logs the runtime of the process to a separate file.

---

## Key Components and Functions

### 1. Hash Functions for MinHash
- **`generate_hash_functions(num_funcs, prime, m)`:**
  - Generates a list of hash function parameters `(a, b)` for hash functions of the form:  
    `h(x) = ((a*hash(x) + b) % prime) % m`
  - Uses a fixed random seed to ensure reproducibility.

### 2. MinHash Signature Computation
- **`minhash_signature(users_set, hash_funcs, prime, m)`:**
  - Computes the MinHash signature for a given set of user IDs.
  - For each hash function, the minimum hash value over the user set is selected.

### 3. Banding for LSH
- **`get_band_signatures(signature, n_bands, n_rows)`:**
  - Splits the full MinHash signature into multiple bands.
  - Each band is represented as a tuple of consecutive hash values.

### 4. Similarity Computation
- **`jaccard_similarity(set1, set2)`:**
  - Computes the Jaccard similarity between two sets.
  - Returns the ratio of the size of the intersection to the size of the union.

### 5. Main Processing (`main` function)
- **Input Processing:**
  - Loads JSON records and creates an RDD.
  - Maps each record to a tuple `(business_id, user_id)`.
  - Groups records by `business_id` to form baskets of user IDs.

- **MinHash and LSH:**
  - Computes the MinHash signature for each business using the generated hash functions.
  - Splits each signature into bands and uses these to group businesses that share a band signature.
  - Generates candidate pairs from groups sharing the same band signature.
  - Writes the candidate pairs (business pairs) to a candidate file.

- **Jaccard Similarity Filtering:**
  - Uses the precomputed baskets (user sets per business) to compute the Jaccard similarity for each candidate pair.
  - Filters out candidate pairs whose similarity is below the given threshold.
  - Writes the final similar business pairs (including similarity scores) to the output file.

- **Runtime Logging:**
  - The total runtime of the process is computed and written to a specified time file.

---

## How to Run

The task is run from the command line and accepts several arguments:

- `--input_file`: Path to the input JSON file containing review records.
- `--candidate_file`: Path where candidate business pairs will be saved.
- `--output_file`: Path where the final output (similar pairs with similarity scores) will be saved.
- `--time_file`: Path where the runtime information will be written.
- `--threshold`: Jaccard similarity threshold for filtering candidate pairs.
- `--n_bands`: Number of bands to split the MinHash signature.
- `--n_rows`: Number of rows per band.

**Example Command:**
```bash
python task.py --input_file ./data/tr1.json --candidate_file ./outputs/candidate.out --output_file ./outputs/task.out --time_file ./outputs/task.time --threshold 0.1 --n_bands 50 --n_rows 2
```

## Summary

This script efficiently identifies similar businesses by:

- Computing MinHash signatures for user sets.
- Using LSH to quickly narrow down candidate pairs.
- Filtering candidate pairs using Jaccard similarity.
- Saving both intermediate candidate pairs and final results along with runtime metrics.
