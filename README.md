# Keyword Volume Analysis Tool

A Streamlit application that retrieves keyword data, search volumes, and competition metrics for URLs using the DataForSEO API.

## Requirements

- Python 3.7+
- DataForSEO API credentials

## Setup

1. Install required packages:
```bash
pip install -r requirements.txt
```

2. Create a `.env` file with your DataForSEO credentials:
```
DATAFORSEO_LOGIN=your_login
DATAFORSEO_PASSWORD=your_password
```

## Usage

1. Prepare your input CSV file with a column named 'url':
```
url
https://example.com
https://example.org
```

2. Run the Streamlit app:
```bash
streamlit run app.py
```

3. Upload your CSV file through the web interface

4. View the results in the app or download them as a CSV file

## Output Format

The tool will return the following data for each URL:
- URL
- Keywords
- Monthly search volume
- Competition level
