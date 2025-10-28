# 🚀 Big Data Projects Portfolio

This repository showcases a collection of **Big Data and Distributed Computing** projects implemented using **Apache Spark**, **Hadoop MapReduce**, and **Python**.  
Each project demonstrates scalable data processing, analytics, and machine learning techniques — reflecting strong skills in **data engineering**, **Spark programming**, and **information retrieval**.

---

## 🧩 Projects Included

### 🧠 1. Named Entity Recognition using PySpark & spaCy

**Goal:** Identify and rank the most frequent named entities (people, organizations, and locations) in large text data using distributed processing.

#### 🔍 Overview
This project demonstrates distributed text processing using **Apache Spark (PySpark)** integrated with **spaCy** for Named Entity Recognition (NER).  
It downloads a text corpus from *Project Gutenberg*, performs entity extraction in a distributed environment, and outputs the top 20 most frequent entities such as people, organizations, and locations.

#### ⚙️ Workflow
1. **Data Collection:** Downloads text data using `wget`.  
2. **RDD Initialization:** Loads and parallelizes data using Spark RDD and DataFrame.  
3. **Entity Extraction:** Uses `spaCy`’s `en_core_web_sm` model to extract entities (`PERSON`, `ORG`, `GPE`, `LOC`).  
4. **Aggregation:** Performs `map()` and `reduceByKey()` transformations to count occurrences.  
5. **Results:** Displays the top 20 most common entities.

#### 🧰 Tech Stack
- **PySpark** – Distributed data processing  
- **spaCy** – Named Entity Recognition  
- **Spark NLP** – Text processing  
- **Python 3.8+**

#### 🧾 Sample Output
Highest first 10 occuring Named-Entities

[('Jo', 1342),
 ('Meg', 674),
 ('Amy', 654),
 ('Laurie', 599),
 ('Beth', 479),
 ('John', 148),
 ('Hannah', 120),
 ('Laurence', 93),
 ('Brooke', 91),
 ('Bhaer', 90)]
---

### 🎬 2. Movie Recommendation System using PySpark (TF–IDF + Cosine Similarity)

**Goal:** Build a scalable, content-based movie recommendation engine that retrieves the most relevant movies using **TF-IDF weighting** and **cosine similarity**.

#### 🔍 Overview
This project implements a **content-based movie recommendation engine** using **PySpark**.  
It processes movie plot summaries from the **CMU Movie Summaries Dataset** and ranks movies by similarity to user search queries.

Unlike collaborative filtering, this system recommends based on the *words and context* within each movie’s plot summary.

#### ⚙️ Workflow
1. **Data Acquisition:** Automatically downloads and extracts the CMU Movie Summaries dataset.  
2. **Data Cleaning:** Tokenizes, removes stopwords, and normalizes text using NLTK and Spark functions.  
3. **TF–IDF Calculation:** Calculates **term frequency (TF)** and **inverse document frequency (IDF)** using Spark DataFrames.  
4. **Query Handling:** Supports both single-term and multi-term search queries.  
5. **Similarity Computation:** Uses **cosine similarity** to compare the query with movie plots and rank the top results.

#### 🧰 Tech Stack
- **Apache Spark (PySpark)** – Large-scale computation  
- **NLTK** – Stopword removal and tokenization  
- **Python 3.8+**  
- **Math (Cosine Similarity)** – Ranking movie relevance  

#### 🧾 Example Output
**Single term Query:** `iron`
<img width="789" height="335" alt="image" src="https://github.com/user-attachments/assets/9e478834-4c03-4c50-9eec-8530fcde0b2f" />


**Multi term Query:** `mystery with detectives and investigation`
<img width="832" height="357" alt="image" src="https://github.com/user-attachments/assets/6523b500-6d97-4f58-bc9b-0b831722946d" />


#### 🧠 Key Learnings
- Implemented **TF-IDF** and **cosine similarity** manually using PySpark  
- Calculated **term frequency (TF)** and **inverse document frequency (IDF)** using Spark DataFrames  
- Used **cosine similarity** to compare search queries with movie plots  
- Applied **data cleaning**, **tokenization**, and **stopword removal** at scale  
- Combined all steps — from preprocessing to ranking — in a distributed Spark pipeline  


