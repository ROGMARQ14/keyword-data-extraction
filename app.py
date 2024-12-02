import streamlit as st
import pandas as pd
from dataforseo_client import RestClient
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize API client with your DataForSEO credentials
client = RestClient(os.getenv('DATAFORSEO_LOGIN'), os.getenv('DATAFORSEO_PASSWORD'))

def process_url(url, location_code=2840):
    """Process a single URL to get keyword data"""
    post_data = dict()
    post_data[len(post_data)] = dict(
        location_code=location_code,  # Default to US
        target=url,
        target_type="url",
        language_name="English"
    )
    
    try:
        response = client.post("/v3/keywords_data/google_ads/keywords_for_site/live", post_data)
        results = []
        
        if response.get("status_code") == 20000:
            tasks = response.get("tasks", [])
            if tasks and "result" in tasks[0]:
                for item in tasks[0]["result"]:
                    results.append({
                        "url": url,
                        "keyword": item.get("keyword"),
                        "search_volume": item.get("search_volume", 0),
                        "competition": item.get("competition", 0)
                    })
        return results
    except Exception as e:
        st.error(f"Error processing {url}: {str(e)}")
        return []

def main():
    st.title("Keyword Volume Analysis Tool")
    st.write("Upload a CSV file containing URLs to analyze their keywords and search volumes.")
    
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
    
    if uploaded_file:
        try:
            df = pd.read_csv(uploaded_file)
            
            if 'url' not in df.columns:
                st.error("CSV file must contain a 'url' column")
                return
            
            urls = df['url'].tolist()
            
            with st.spinner('Processing URLs...'):
                all_results = []
                progress_bar = st.progress(0)
                
                for idx, url in enumerate(urls):
                    results = process_url(url)
                    all_results.extend(results)
                    progress_bar.progress((idx + 1) / len(urls))
                
                if all_results:
                    results_df = pd.DataFrame(all_results)
                    
                    st.write("### Results")
                    st.dataframe(results_df)
                    
                    csv = results_df.to_csv(index=False)
                    st.download_button(
                        "Download Results",
                        csv,
                        "keyword_analysis_results.csv",
                        "text/csv",
                        key='download-csv'
                    )
                else:
                    st.warning("No results found for the provided URLs.")
                    
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")

if __name__ == "__main__":
    main()
