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

 
#### 📁 Folder
[`/NER_PySpark_SpaCy`](./NER_PySpark_SpaCy)

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
-----------------------------------------------------------------
Rank | Movie Title                               | TF-IDF score
-----------------------------------------------------------------
1    | Kid with the Golden Arm                 | 110.8784
2    | The Adventure of Iron Pussy             | 91.5952
3    | Iron Monkey                             | 91.5952
4    | Iron Invader                            | 91.5952
5    | Iron Monkey                             | 81.9536
6    | Pokémon 4Ever                           | 43.3872
7    | Princess Mononoke                       | 43.3872
8    | Old Rockin' Chair Tom                   | 33.7456
9    | Next Avengers: Heroes of Tomorrow       | 28.9248
10   | The Man in the Iron Mask                | 28.9248

**Multi term Query:** `mystery with detectives and investigation`
-----------------------------------------------------------------
Rank | Movie Title                               | Cosine Similarity
-----------------------------------------------------------------
1    | Mystery Team                            | 0.3064
2    | The Blue Mansion                        | 0.2747
3    | I Thought About You                     | 0.2668
4    | The Tigers                              | 0.2253
5    | Jigsaw                                  | 0.2164
6    | Mataharis                               | 0.1975
7    | Sahasram                                | 0.1892
8    | The Shadow                              | 0.1889
9    | Accused                                 | 0.1869
10   | The Dead Talk Back                      | 0.1842

#### 🧠 Key Learnings
- Implemented **TF-IDF** and **cosine similarity** manually using PySpark  
- Calculated **term frequency (TF)** and **inverse document frequency (IDF)** using Spark DataFrames  
- Used **cosine similarity** to compare search queries with movie plots  
- Applied **data cleaning**, **tokenization**, and **stopword removal** at scale  
- Combined all steps — from preprocessing to ranking — in a distributed Spark pipeline  


