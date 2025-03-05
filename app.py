import streamlit as st
import pandas as pd
from client import RestClient
import os
import time

def submit_keywords_task(keywords, client, location_code=2840):
    """Submit a task to process a list of keywords to get search volume data"""
    post_data = dict()
    post_data[len(post_data)] = dict(
        location_code=location_code,  # Default to US
        keywords=keywords,
        language_name="English"
    )
    
    try:
        # Changed endpoint from 'live' to 'task_post'
        response = client.post("/v3/keywords_data/google_ads/search_volume/task_post", post_data)
        
        if response and isinstance(response, dict):
            if response.get("status_code") == 20000:
                tasks = response.get("tasks", [])
                if tasks and isinstance(tasks, list) and len(tasks) > 0:
                    task_id = tasks[0].get("id")
                    return task_id
        
        st.error(f"Failed to submit task: {response.get('status_message', 'Unknown error')}")
        return None
    except Exception as e:
        st.error(f"Error submitting keywords task: {str(e)}")
        return None

def get_task_results(task_id, client):
    """Get the results of a previously submitted task"""
    if not task_id:
        return []
    
    try:
        # Get task results using the task_get endpoint
        response = client.get(f"/v3/keywords_data/google_ads/search_volume/task_get/{task_id}")
        results = []
        
        if response and isinstance(response, dict):
            if response.get("status_code") == 20000:
                tasks = response.get("tasks", [])
                if tasks and isinstance(tasks, list) and len(tasks) > 0:
                    task = tasks[0]
                    status = task.get("status_code")
                    
                    # Check if task is still in progress
                    if status == 40401:  # Task in progress
                        return "in_progress"
                        
                    # Check if task is completed
                    if status == 20000:
                        result = task.get("result", [])
                        if isinstance(result, list):
                            for item in result:
                                if isinstance(item, dict):
                                    results.append({
                                        "keyword": item.get("keyword", ""),
                                        "search_volume": item.get("search_volume", 0),
                                        "competition": item.get("competition", 0)
                                    })
        
        return results
    except Exception as e:
        st.error(f"Error getting task results: {str(e)}")
        return []

def process_keywords(keywords, client, location_code=2840):
    """Process a list of keywords to get search volume data using task-based approach"""
    # Submit task
    task_id = submit_keywords_task(keywords, client, location_code)
    
    if not task_id:
        return [{"keyword": k, "search_volume": 0, "competition": 0, "note": "Failed to submit task"} 
                for k in keywords]
    
    # Poll for results
    max_attempts = 30  # Maximum number of attempts to check task status
    poll_interval = 2  # Time in seconds between status checks
    
    for attempt in range(max_attempts):
        results = get_task_results(task_id, client)
        
        # If task is still in progress, wait and try again
        if results == "in_progress":
            # Show progress message with attempt count
            st.text(f"Task in progress, polling attempt {attempt+1}/{max_attempts}...")
            time.sleep(poll_interval)
            continue
        
        # If we have results, return them
        if results:
            # Add keywords that didn't return data
            processed_keywords = {r["keyword"] for r in results}
            for keyword in keywords:
                if keyword not in processed_keywords:
                    results.append({
                        "keyword": keyword,
                        "search_volume": 0,
                        "competition": 0,
                        "note": "No data found"
                    })
            return results
    
    # If we've exhausted our attempts, return error
    st.error(f"Task did not complete after {max_attempts} polling attempts")
    return [{"keyword": k, "search_volume": 0, "competition": 0, "note": "Task timeout"} 
            for k in keywords]

def main():
    st.title("Keyword Volume Analysis Tool")
    st.write("Enter your DataForSEO credentials and upload a CSV file containing keywords to analyze their search volumes and competition.")
    
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
                
                # Get keywords from the first column regardless of its name
                keywords = df.iloc[:, 0].tolist()
                
                if not keywords:
                    st.error("No keywords found in the first column of the CSV file")
                    return
                
                # Remove any empty or NaN values and strip whitespace
                keywords = [str(k).strip() for k in keywords if pd.notna(k) and str(k).strip()]
                
                if not keywords:
                    st.error("No valid keywords found in the file")
                    return
                
                st.write(f"Processing {len(keywords)} keywords...")
                
                with st.spinner('Getting search volumes...'):
                    # Process keywords in batches of 100 (API limit)
                    batch_size = 100
                    all_results = []
                    progress_bar = st.progress(0)
                    total_batches = (len(keywords) + batch_size - 1) // batch_size
                    
                    for i in range(0, len(keywords), batch_size):
                        batch = keywords[i:i + batch_size]
                        results = process_keywords(batch, client)
                        all_results.extend(results)
                        progress_bar.progress((i + len(batch)) / len(keywords))
                    
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
                        st.warning("No results found for any of the provided keywords.")
                        
            except Exception as e:
                st.error(f"Error processing file: {str(e)}")
    else:
        st.info("Please enter your DataForSEO credentials in the sidebar to proceed.")

if __name__ == "__main__":
    main()
