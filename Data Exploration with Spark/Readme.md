# README

This repository contains two PySpark scripts designed for processing and analyzing review data from JSON files. Each script takes command-line arguments to specify input files, output destinations, and various parameters. Below is an overview of what each code does.

---

## Task 1: Review Analysis and Word Frequency Extraction

**Purpose:**  
This script processes a JSON file of reviews to extract several metrics and perform text analysis on the review texts. It outputs a JSON file containing computed results.

**Key Steps and Functionalities:**

1. **Spark Context Configuration:**  
   - Initializes a PySpark context with specified settings such as application name, master node configuration, and memory allocations for the driver and executors.

2. **Argument Parsing:**  
   - Uses `argparse` to allow customization of:
     - Input review file (`--input_file`)
     - Output file (`--output_file`)
     - Stopwords file (`--stopwords`)
     - Review threshold `m` for counting user reviews (`--m`)
     - Star rating filter (`--s`)
     - Number of top frequent words to extract (`--i`)
     - Target year for filtering reviews (`--y`)

3. **Helper Functions:**  
   - `is_review_in_year(review, year)`: Checks if a review’s date matches a specified year.
   - `text_preprocessing(txt, stopwords)`:  
     - Converts text to lowercase.
     - Splits text into words and removes common punctuation.
     - Filters out empty strings and words found in the stopwords list.
   - `extract_year(date_string)`:  
     - Converts a date string (in the format `%Y-%m-%d %H:%M:%S`) into a year string.  
     - Returns `None` if the date format is invalid.

4. **Data Processing:**
   - **A. Count of Distinct Businesses:**  
     Extracts unique business IDs from the reviews.
   - **B. Count of Users with More Than `m` Reviews:**  
     Aggregates the number of reviews per user and counts users with more than the threshold `m`.
   - **C. Review Counts per Year:**  
     Maps reviews to their corresponding year and computes the total count for each year.
   - **D. Count of Reviews with Specific Star Rating:**  
     Filters reviews that exactly match the given star rating (`--s`).
   - **E. Top Frequent Words in Reviews for a Given Year:**  
     - Filters reviews based on the specified year.
     - Preprocesses the review texts (removing stopwords and punctuation).
     - Counts the frequency of each word and returns the top `i` words based on frequency (with alphabetical order used as a tiebreaker).

5. **Output:**  
   - The results for each part are stored in a dictionary with keys `"A"`, `"B"`, `"C"`, `"D"`, and `"E"`.
   - The output dictionary is written to a JSON file specified by the `--output_file` argument.

6. **Debugging Information:**  
   - The script includes print statements to display the number of reviews after filtering by year and after the flatMap operation for word processing.

---

## Code 2: Category-Based Average Star Rating

**Purpose:**  
This script joins review data with business data to calculate the average star ratings for each business category and then extracts the top categories based on these averages.

**Key Steps and Functionalities:**

1. **Spark Context Configuration:**  
   - Similar to Code 1, a PySpark context is created with specific configurations such as application name, master node settings, and memory allocations.

2. **Argument Parsing:**  
   - Uses `argparse` to customize:
     - Review file (`--review_file`)
     - Business file (`--business_file`)
     - Output file (`--output_file`)
     - Number `n` of top categories to output (`--n`)

3. **Loading and Processing Business Data:**
   - Reads the business JSON file.
   - Filters out businesses that do not have a `categories` field.
   - Splits the `categories` string into individual categories.  
   - Creates multiple entries for a business if it belongs to more than one category by outputting tuples of `(business_id, category)`.

4. **Loading and Processing Review Data:**
   - Reads the review JSON file.
   - Extracts a tuple consisting of the `business_id` and a tuple containing the star rating (and optionally the review text).
   - Removes duplicate reviews and maps the tuple to only keep the star rating for each review.

5. **Joining Datasets:**
   - Joins the review data with the business data on the `business_id` key.
   - The join results in pairs of (business_id, (stars, category)).

6. **Category Average Calculation:**
   - Maps the joined RDD to tuples of `(category, (stars, 1))`.
   - Uses `reduceByKey` to sum the stars and count the reviews for each category.
   - Computes the average star rating per category by dividing the total stars by the count of reviews.

7. **Sorting and Output:**
   - The resulting categories are sorted by average star rating in descending order. In case of ties, categories are sorted alphabetically.
   - The top `n` categories are selected.
   - The final results are stored in a JSON object under the key `"result"` and written to the specified output file.

---

## Usage

Both scripts are executed from the command line. For example:

**Running Task 1:**
```bash
python task1.py --input_file path/to/review.json --output_file output.json --stopwords path/to/stopwords --m 10 --s 2 --i 10 --y 2018
```

**Running Task 2:**
```bash
python task2.py --review_file path/to/review.json --business_file path/to/business.json --output_file output.json --n 50
```

Each argument can be adjusted to suit the data and desired analysis parameters.

