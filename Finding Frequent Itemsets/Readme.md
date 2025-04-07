# README

This repository contains two tasks that implement market-basket analysis using distributed computing with PySpark. Both tasks use variations of the SON (Savasere, Omiecinski, and Navathe) algorithm to efficiently discover frequent itemsets in large datasets. The implementations incorporate key concepts from the A-Priori and PCY algorithms to generate candidate itemsets and validate their frequency.

---

## Task 1: SON Algorithm with A-Priori

**Overview:**  
Task 1 implements the SON algorithm combined with the A-Priori method for frequent itemset mining. It is designed to work on CSV input data containing records with a year, user, and business. The goal is to discover frequent itemsets from baskets grouped by either user or business, depending on the specified case.

**Key Functionalities:**

1. **Data Preprocessing:**
   - **Header Removal and Filtering:**  
     The input CSV file is processed by removing the header and filtering records by a given year.
   - **Parsing and Grouping:**  
     Depending on the case parameter:
     - *Case 1:* Creates baskets mapping users to a sorted list of businesses.
     - *Case 2:* Creates baskets mapping businesses to a sorted list of users.

2. **Local Candidate Generation (SON Phase 1):**
   - **A-Priori Algorithm:**  
     Runs on each data partition:
     - Computes a partition-specific support threshold.
     - Counts singleton items and iteratively generates larger candidate itemsets.
     - Uses the A-Priori property (all subsets of a candidate must be frequent) to prune candidate itemsets.
   - The local candidates from each partition are then collected and de-duplicated.

3. **Global Validation (SON Phase 2):**
   - **Broadcasting Candidates:**  
     The locally discovered candidates are broadcast to all nodes.
   - **Candidate Validation:**  
     Each basket is checked to count the occurrences of each candidate.
   - **Filtering:**  
     Candidates that meet the global support threshold are selected as frequent itemsets.

4. **Output Formatting:**
   - The candidates and frequent itemsets are grouped by itemset size.
   - The output JSON file includes:
     - Grouped candidate itemsets.
     - Grouped frequent itemsets with their support counts.
     - Total runtime for the process.

---

## Task 2: SON Algorithm with PCY

**Overview:**  
Task 2 also implements the SON algorithm but uses the PCY (Park-Chen-Yu) algorithm in the candidate generation phase to optimize the discovery of size-2 itemsets. This task is tailored for market-basket analysis on Yelp-like data where baskets represent users and the set of businesses they reviewed.

**Key Functionalities:**

1. **Data Preprocessing:**
   - **Header Removal:**  
     The input CSV file is processed by removing the header.
   - **Building Baskets:**  
     The data is parsed and grouped to create user baskets, where each basket consists of the set of businesses reviewed by a user.
   - **Basket Filtering:**  
     Only users who have reviewed more than a specified number of businesses (filter threshold) are retained.

2. **Local Candidate Generation with PCY (SON Phase 1):**
   - **Singleton Counting and Hashing:**  
     For each basket:
     - Singleton item frequencies are counted.
     - Pairs of items are hashed into a hash table to count pair frequencies.
   - **Bucket Filtering:**  
     Buckets with counts exceeding the partition-specific threshold are marked as frequent.
   - **Candidate Pair Generation:**  
     Candidate pairs are generated from frequent singletons, ensuring that the hash bucket for each pair is also frequent.
   - **Higher-Order Candidates:**  
     The algorithm iteratively generates larger candidate itemsets (using a-priori style candidate generation with PCY filtering for pairs) until no new candidates are found.

3. **Global Validation (SON Phase 2):**
   - **Broadcasting Candidates:**  
     Local candidates are broadcast to all nodes.
   - **Candidate Validation:**  
     Each basket is examined to count the occurrences of each candidate.
   - **Filtering:**  
     Global support counts are aggregated, and only itemsets meeting the support threshold are retained as frequent.

4. **Output Formatting:**
   - The candidates and frequent itemsets are grouped by itemset size.
   - The final output JSON includes:
     - Grouped candidate itemsets.
     - Grouped frequent itemsets with their corresponding support counts.
     - Total runtime for the analysis.

---

## Common Components and Execution Flow

- **Algorithm Phases:**
  - **Phase 1 (Local Candidate Generation):**  
    Each task runs a local candidate generation algorithm (A-Priori in Task 1 and PCY in Task 2) on partitions of the data to reduce the candidate search space.
  - **Phase 2 (Global Validation):**  
    The candidates are then validated across all data partitions using a distributed reduce operation.
  
- **Output:**  
  Both tasks output a JSON file containing:
  - Grouped candidate itemsets.
  - Grouped frequent itemsets (with support counts).
  - Runtime information.

- **Execution:**  
  Each task accepts command-line arguments to specify:
  - Input file path
  - Output file path
  - Filtering thresholds (e.g., year filter, support threshold, filter threshold for baskets)
  - Case selection (for Task 1)
  - Hash table size (for Task 2)

---

## How to Run the Tasks

**Task 1 Example:**
```bash
python task1.py --y 2017 --c 1 --t 10 --input_file ../data/small2.csv --output_file ./HW2task1.json
```

**Task 2 Example:**
```bash
python task2.py --f 3 --t 10 --input_file ../data/your_input.csv --output_file ./output_task2.json
```

Adjust the arguments as needed to match your dataset and analysis requirements.
