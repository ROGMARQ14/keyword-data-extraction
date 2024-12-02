import streamlit as st
import pandas as pd
from client import RestClient
import os

def process_url(url, client, location_code=2840):
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
        
        if response and isinstance(response, dict):
            if response.get("status_code") == 20000:
                tasks = response.get("tasks", [])
                if tasks and isinstance(tasks, list) and len(tasks) > 0:
                    result = tasks[0].get("result", [])
                    if isinstance(result, list):
                        for item in result:
                            if isinstance(item, dict):
                                results.append({
                                    "url": url,
                                    "keyword": item.get("keyword", ""),
                                    "search_volume": item.get("search_volume", 0),
                                    "competition": item.get("competition", 0)
                                })
        
        if not results:
            st.warning(f"No keyword data found for URL: {url}")
            results.append({
                "url": url,
                "keyword": "No data found",
                "search_volume": 0,
                "competition": 0
            })
            
        return results
    except Exception as e:
        st.error(f"Error processing {url}: {str(e)}")
        return [{
            "url": url,
            "keyword": f"Error: {str(e)}",
            "search_volume": 0,
            "competition": 0
        }]

def main():
    st.title("Keyword Volume Analysis Tool")
    st.write("Enter your DataForSEO credentials and upload a CSV file containing URLs to analyze their keywords and search volumes.")
    
    # DataForSEO credentials input
    with st.sidebar:
        st.header("DataForSEO Credentials")
        dataforseo_login = st.text_input("DataForSEO Login", type="default")
        dataforseo_password = st.text_input("DataForSEO Password", type="password")
    
    # Only show file uploader if credentials are provided
    if dataforseo_login and dataforseo_password:
        # Initialize client with provided credentials
        client = RestClient(dataforseo_login, dataforseo_password)
        
        uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
        
        if uploaded_file:
            try:
                df = pd.read_csv(uploaded_file)
                
                # Get URLs from the first column regardless of its name
                urls = df.iloc[:, 0].tolist()
                
                if not urls:
                    st.error("No URLs found in the first column of the CSV file")
                    return
                
                with st.spinner('Processing URLs...'):
                    all_results = []
                    progress_bar = st.progress(0)
                    total_urls = len(urls)
                    
                    for idx, url in enumerate(urls):
                        if pd.notna(url):  # Skip empty or NaN values
                            results = process_url(url.strip(), client)  # Strip whitespace
                            all_results.extend(results)
                        progress_bar.progress((idx + 1) / total_urls)
                    
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
                        st.warning("No results found for any of the provided URLs.")
                        
            except Exception as e:
                st.error(f"Error processing file: {str(e)}")
    else:
        st.info("Please enter your DataForSEO credentials in the sidebar to proceed.")

if __name__ == "__main__":
    main()
