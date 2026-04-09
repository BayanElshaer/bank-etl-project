# bank-etl-project
# 🏦 Bank Market Capitalization ETL Pipeline

## 📌 Overview

This project implements a complete **ETL (Extract, Transform, Load) pipeline** that collects financial data about the world's largest banks from a Wikipedia page, processes it, and stores it for analysis.

The pipeline demonstrates real-world data engineering tasks including **web scraping, data cleaning, transformation, and storage**.

---

## ⚙️ Features

* 🔍 Extracts tabular data from a web page using **BeautifulSoup**
* 🧹 Cleans and processes raw data using **pandas**
* 💱 Converts market capitalization into multiple currencies:

  * GBP 🇬🇧
  * EUR 🇪🇺
  * INR 🇮🇳
* 💾 Stores processed data into:

  * CSV file
  * SQLite database
* 🧾 Executes SQL queries to retrieve insights
* 📝 Logs all pipeline steps using Python `logging`

---

## 🛠️ Technologies Used

* Python
* Pandas
* NumPy
* BeautifulSoup (bs4)
* Requests
* SQLite
* Logging

---

## 📂 Project Structure

```
bank-etl-project/
│
├── main.py              # Main ETL pipeline script
├── banks.csv            # Output CSV file
├── etl.log              # Log file
├── requirements.txt     # Project dependencies
└── README.md            # Project documentation
```

---

## 🔄 ETL Process

### 1. Extract

* Fetches HTML content from a Wikipedia page
* Parses and extracts bank data (Rank, Name, Market Cap)

### 2. Transform

* Cleans numeric values (removes commas)
* Converts data types
* Calculates market cap in:

  * GBP
  * EUR
  * INR

### 3. Load

* Saves data to CSV file
* Stores data in SQLite database

---

## ▶️ How to Run

### 1. Clone the repository

```
git clone https://github.com/YOUR_USERNAME/bank-etl-project.git
cd bank-etl-project
```

### 2. Install dependencies

```
pip install -r requirements.txt
```

### 3. Run the project

```
python main.py
```

---

## 📊 Example Output

### SQL Query Result:

```
Bank name           | GBP
--------------------|--------
JPMorgan Chase      | 300.25
Bank of America     | 250.10
...
```

---

## 📈 Skills Demonstrated

* Web Scraping
* Data Cleaning & Transformation
* ETL Pipeline Design
* Database Integration (SQLite)
* Writing and executing SQL queries
* Logging and debugging

---

## 🎯 Purpose

This project was built as part of a learning journey in **Data Engineering and Data Analysis**, showcasing the ability to handle real-world data workflows.

---

## 🚀 Future Improvements

* Add support for APIs instead of web scraping
* Automate pipeline using scheduling tools (e.g., cron jobs)
* Add data visualization (matplotlib / seaborn)
* Deploy as a web service

---

## 👤 Author

**Byan Alshaer**

---

## ⭐ If you like this project

Feel free to ⭐ the repository and share it!
