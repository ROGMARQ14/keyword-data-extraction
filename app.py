import streamlit as st
import pandas as pd
from client import RestClient
import os
import time
from dotenv import load_dotenv, find_dotenv

# Load environment variables from .env file
env_file = find_dotenv(usecwd=True)
if env_file:
    load_dotenv(env_file)
    env_loaded = True
else:
    env_loaded = False

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
        response = client.post("v3/keywords_data/google_ads/search_volume/task_post", post_data)
        
        if response and isinstance(response, dict):
            if response.get("status_code") == 20000:
                tasks = response.get("tasks", [])
                if tasks and isinstance(tasks, list) and len(tasks) > 0:
                    task_id = tasks[0].get("id")
                    return task_id
            else:
                status_code = response.get("status_code")
                status_message = response.get("status_message", "Unknown error")
                st.error(f"API Error {status_code}: {status_message}")
        
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
        response = client.get(f"v3/keywords_data/google_ads/search_volume/task_get/{task_id}")
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
                    else:
                        st.warning(f"Task status: {status} - {task.get('status_message', '')}")
        
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
    
    st.success(f"Task submitted successfully. Task ID: {task_id}")
    
    # Poll for results
    max_attempts = 60  # Increased maximum number of attempts
    poll_interval = 5  # Increased time between checks to reduce API load
    poll_backoff = 1.5  # Backoff multiplier to gradually increase wait time
    current_interval = poll_interval
    
    progress_placeholder = st.empty()
    
    for attempt in range(max_attempts):
        # Update the interval with backoff strategy
        if attempt > 5:  # After 5 attempts, start increasing the interval
            current_interval = min(30, poll_interval * (poll_backoff ** (attempt - 5)))
        
        progress_placeholder.text(f"Task in progress, polling attempt {attempt+1}/{max_attempts}... (waiting {current_interval:.1f}s)")
        
        results = get_task_results(task_id, client)
        
        # If task is still in progress, wait and try again
        if results == "in_progress":
            time.sleep(current_interval)
            continue
        
        # If we have results, return them
        if results:
            progress_placeholder.empty()
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

def process_large_keyword_list(keywords, client, status_container, results_container, progress_bar):
    """Process a large list of keywords efficiently by submitting multiple tasks in parallel
    and aggregating results as they become available."""
    
    # Determine optimal batch size
    batch_size = 1000  # Maximum supported by the API
    
    # Calculate number of batches
    total_batches = (len(keywords) + batch_size - 1) // batch_size
    
    # Create a placeholder for each batch's status
    batch_statuses = status_container.empty()
    task_ids = []
    completed_batches = 0
    all_results = []
    
    # First, submit all tasks
    with status_container:
        st.write(f"Submitting {total_batches} batch(es) of keywords...")
        
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i:i + batch_size]
            batch_num = i // batch_size + 1
            
            st.text(f"Submitting batch {batch_num}/{total_batches} ({len(batch)} keywords)...")
            task_id = submit_keywords_task(batch, client)
            
            if task_id:
                task_ids.append((task_id, batch, batch_num))
                st.success(f"Batch {batch_num} submitted. Task ID: {task_id}")
            else:
                st.error(f"Failed to submit batch {batch_num}")
                # Add failed keywords with error note
                all_results.extend([{"keyword": k, "search_volume": 0, "competition": 0, "note": "Failed to submit task"} 
                                    for k in batch])
                completed_batches += 1
                progress_bar.progress(completed_batches / total_batches)
    
    # Now poll for results from all tasks
    if task_ids:
        st.write(f"Polling for results from {len(task_ids)} tasks...")
        
        max_attempts = 60  # Maximum polling attempts per task
        poll_interval = 5  # Starting interval between polls (seconds)
        max_interval = 30  # Maximum interval (won't go beyond this)
        
        # Keep track of which tasks are still in progress
        pending_tasks = task_ids.copy()
        
        attempt = 0
        while pending_tasks and attempt < max_attempts:
            attempt += 1
            
            # Calculate current poll interval with backoff
            current_interval = min(max_interval, poll_interval * (1.2 ** min(10, attempt - 1)))
            
            # Update status display
            status_text = f"Polling attempt {attempt}/{max_attempts}\n"
            status_text += f"Waiting {current_interval:.1f}s between polls\n"
            status_text += f"Remaining batches: {len(pending_tasks)}/{len(task_ids)}\n"
            
            batch_statuses.text(status_text)
            
            # Check each pending task
            still_pending = []
            for task_id, batch, batch_num in pending_tasks:
                results = get_task_results(task_id, client)
                
                if results == "in_progress":
                    still_pending.append((task_id, batch, batch_num))
                    continue
                
                if results:
                    # Process successful results
                    processed_keywords = {r["keyword"] for r in results}
                    
                    # Add any missing keywords
                    for keyword in batch:
                        if keyword not in processed_keywords:
                            results.append({
                                "keyword": keyword,
                                "search_volume": 0,
                                "competition": 0,
                                "note": "No data found"
                            })
                    
                    all_results.extend(results)
                    status_container.success(f"Batch {batch_num} completed: {len(results)} results")
                else:
                    # Handle failed task
                    failed_results = [{"keyword": k, "search_volume": 0, "competition": 0, 
                                      "note": "Task failed to return results"} for k in batch]
                    all_results.extend(failed_results)
                    status_container.error(f"Batch {batch_num} failed to return results")
                
                # Update progress
                completed_batches += 1
                progress_bar.progress(completed_batches / total_batches)
                
                # If we have some results, show a preview in the results container
                if len(all_results) > 0 and completed_batches % 2 == 0:
                    with results_container:
                        preview_df = pd.DataFrame(all_results[:100])  # Just show first 100 for preview
                        st.write(f"### Preview of results ({len(all_results)} keywords processed so far)")
                        st.dataframe(preview_df)
            
            # Update pending tasks
            pending_tasks = still_pending
            
            # If there are still pending tasks, wait before next polling attempt
            if pending_tasks:
                time.sleep(current_interval)
        
        # Handle any remaining pending tasks as timeouts
        if pending_tasks:
            for task_id, batch, batch_num in pending_tasks:
                timeout_results = [{"keyword": k, "search_volume": 0, "competition": 0, "note": "Task timeout"} 
                                 for k in batch]
                all_results.extend(timeout_results)
                status_container.warning(f"Batch {batch_num} timed out")
                
                # Update progress for timed out batches
                completed_batches += 1
                progress_bar.progress(completed_batches / total_batches)
    
    return all_results

def main():
    st.title("Keyword Volume Analysis Tool")
    st.write("Enter your DataForSEO credentials and upload a CSV file containing keywords to analyze their search volumes and competition.")
    
    # Try to load DataForSEO credentials from environment variables
    default_login = os.getenv("DATAFORSEO_LOGIN", "")
    default_password = os.getenv("DATAFORSEO_PASSWORD", "")
    
    # DataForSEO credentials input
    with st.sidebar:
        st.header("DataForSEO Credentials")
        dataforseo_login = st.text_input("DataForSEO Login", value=default_login)
        dataforseo_password = st.text_input("DataForSEO Password", value=default_password, type="password")
        
        # Add advanced settings
        st.header("Advanced Settings")
        use_optimized_mode = st.checkbox("Use Optimized Mode for Large Keyword Lists", value=True)
        
        # Add debug info in sidebar
        if st.checkbox("Show Debug Info"):
            st.subheader("Environment Variables")
            if env_loaded:
                st.success(f"Found and loaded .env file: {env_file}")
            else:
                st.warning("No .env file found. You can still use the app with manually entered credentials.")
                
            st.text("Environment variables from .env file:")
            st.text(f"DATAFORSEO_LOGIN from .env: {'Set' if default_login else 'Not Set'}")
            st.text(f"DATAFORSEO_PASSWORD from .env: {'Set' if default_password else 'Not Set'}")
            
            st.info("""
            Note: "Not Set" only means the .env file doesn't contain these variables.
            The credentials you manually enter above will be used regardless.
            """)
            
            # Show current working credentials
            st.subheader("Active Credentials")
            st.text(f"Using login: {'Set' if dataforseo_login else 'Not Set'}")
            st.text(f"Using password: {'Set' if dataforseo_password else 'Not Set'}")
    
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
                
                # Set up progress tracking
                progress_bar = st.progress(0)
                status_container = st.container()
                results_container = st.container()
                
                if use_optimized_mode and len(keywords) > 200:
                    # Use the optimized parallel processing for large lists
                    with st.spinner('Processing keywords in parallel batches...'):
                        all_results = process_large_keyword_list(
                            keywords, client, status_container, results_container, progress_bar
                        )
                else:
                    # Use the original processing method for smaller lists
                    with st.spinner('Getting search volumes...'):
                        # Process keywords in batches
                        batch_size = min(1000, len(keywords))  # Use up to 1000 keywords per batch
                        all_results = []
                        
                        total_batches = (len(keywords) + batch_size - 1) // batch_size
                        
                        for i in range(0, len(keywords), batch_size):
                            batch = keywords[i:i + batch_size]
                            batch_num = i // batch_size + 1
                            
                            status_container.text(f"Processing batch {batch_num}/{total_batches} ({len(batch)} keywords)...")
                            results = process_keywords(batch, client)
                            all_results.extend(results)
                            progress_bar.progress((i + len(batch)) / len(keywords))
                
                # Process final results
                if all_results:
                    results_df = pd.DataFrame(all_results)
                    
                    with results_container:
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
                        
                        # Summary statistics
                        st.write("### Summary")
                        total_keywords = len(all_results)
                        keywords_with_data = sum(1 for r in all_results if r.get("search_volume", 0) > 0)
                        keywords_no_data = total_keywords - keywords_with_data
                        
                        st.write(f"Total keywords processed: {total_keywords}")
                        st.write(f"Keywords with search volume data: {keywords_with_data} ({keywords_with_data/total_keywords*100:.1f}%)")
                        st.write(f"Keywords with no data: {keywords_no_data} ({keywords_no_data/total_keywords*100:.1f}%)")
                        
                        if keywords_with_data > 0:
                            avg_search_volume = sum(r.get("search_volume", 0) for r in all_results) / keywords_with_data
                            st.write(f"Average search volume: {avg_search_volume:.1f}")
                else:
                    st.warning("No results found for any of the provided keywords.")
                        
            except Exception as e:
                st.error(f"Error processing file: {str(e)}")
                st.exception(e)  # Show detailed error information
    else:
        st.info("Please enter your DataForSEO credentials in the sidebar to proceed.")

if __name__ == "__main__":
    main()
