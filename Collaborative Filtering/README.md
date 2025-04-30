# Item‐Based Collaborative Filtering Recommender

A Spark RDD implementation of an item‐based collaborative filtering system on Yelp review data.  
You’ll first **build** an item–item similarity model from training reviews, then **predict** user ratings on held-out pairs.

---

## 📋 Overview

1. **Preprocessing & Data Structuring**  
   - Read JSON reviews: each record has `user_id`, `business_id`, and `stars`.  
   - Build two helpful groupings:  
     - **User→[(business, rating)]** for generating co-rated item pairs and later prediction.  
     - **Business→[rating]** to compute per‐item averages and global average.

2. **Model Building (`build.py`)**  
   - For each user’s rated businesses, emit every pair of co-rated items along with their two ratings.  
   - Group by **(item₁, item₂)** and keep only pairs with at least _m_ common raters.  
   - Compute the **Pearson correlation** over the paired ratings → similarity `sim`.  
   - Output each pair as one JSON line:
     ```json
     {
       "b1": "BUSINESS_ID_1",
       "b2": "BUSINESS_ID_2",
       "sim": 0.1234,
       "num_common": 42
     }
     ```

3. **Rating Prediction (`predict.py`)**  
   - Load the **item–item model** into memory as a dict  
     `((b1,b2) → (sim, num_common))`.  
   - For each test pair `(user, item)`:  
     1. Fetch all businesses `j` that `user` has rated.  
     2. Look up `(item, j)` or `(j, item)` in the model → `(sim, cnt)`.  
     3. Apply **shrinkage**:  
        ```python
        shrink = cnt / float(cnt + 50)
        w = sim * shrink
        ```  
     4. Keep **only positive** weights, sort by descending `w`, take top _n_ neighbors.  
     5. Compute the weighted‐centered prediction:
        p_{u,i} = r̄ᵤ + (∑ⱼ wᵢⱼ · (rᵤⱼ − r̄ᵤ)) / (∑ⱼ wᵢⱼ)

        where r̄ᵤ is the user’s mean rating.  
     6. **Fallbacks**:  
        - If no positive‐weight neighbors → return r̄ᵤ.  
        - (Optional) If `user` is unseen → return global average (or business average).

4. **Output & Logging**  
   - Both scripts write a simple **time file** (`--time_file`) containing JSON:
     ```json
     { "time": 32.05 }
     ```
   - **build.py** writes the model as a _single_ JSON file at `--model_file`.  
   - **predict.py** writes one JSON line per prediction to `--output_file`:
     ```json
     {
       "user_id": "USER123",
       "business_id": "BUS456",
       "stars": 3.75
     }
     ```

---

## 🔧 Requirements

- **Python 3.9**  
- **Apache Spark 3.2.1** (RDD API only)  
- **pyspark** Python package  
- Java JDK 8+  

Install Spark and then:
```bash
pip install pyspark
```

## 📂 Data Layout

Place your files under `data/`:
- `data/train_review.json` — 80% of reviews for training
- `data/val_review.json` — (user,business) pairs to predict
- `data/val_review_ratings.json` — ground truth for validation

## 🚀 How to Run

1. **Build the model**
```bash
python build.py \
  --train_file data/train_review.json \
  --model_file HW4.model \
  --time_file  HW4.build.time \
  --m 5
```
- `--m` = minimum common raters (try 3, 5, 7)

2. **Predict ratings**
```bash
python predict.py \
  --train_file   data/train_review.json \
  --test_file    data/val_review.json \
  --model_file   HW4.model \
  --output_file  HW4.val.out \
  --time_file    HW4.predict.time \
  --n 8
```
- `--n` = number of neighbors (try 4, 6, 8, 10)

After running, compare `HW4.val.out` against `val_review_ratings.json` to compute RMSE.

## ⚙️ Tuning & Tips

- Positive similarities only: discard `w ≤ 0` to avoid pulling predictions away.
- Sort by `w`, not `|w|`.
- User‐mean fallback: returning r̄ᵤ when no neighbors often beats business/global averages.
- Hyperparameter grid:
  - m ∈ {3, 5, 7}
  - n ∈ {4, 6, 8, 10}
  - Shrinkage constant (in cnt/(cnt+K)) with K ∈ {20, 50, 100}
 
A small grid search on your validation split will typically push RMSE below 0.90 and RMSE₁₂₅ under 1.30.

## 📈 Summary

This project builds an item‐based collaborative filtering recommender using only Spark RDDs and Python standard libraries.
It covers:
- Data structuring (user‐item & item‐user groupings)
- Pairwise co‐rating extraction
- Pearson similarity with co‐rating threshold
- Shrinkage‐weighted neighborhood prediction
- Runtime logging and single‐file model output

