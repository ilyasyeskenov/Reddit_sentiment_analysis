import requests
import praw
import csv
import pandas as pd
from datetime import datetime, timedelta

#Create a Reddit instance
reddit = praw.Reddit(
    client_id='',
    client_secret='',
    user_agent='Powerful-Major6768'
)

def collect_data(keyword, limit_per_request = 1000, total_requests = 5, max_comments = 70):
    """
    Collects submission and comment data from Reddit based on a specified keyword.
    
    This function searches the subreddit 'all' for submissions containing the given keyword,
    retrieves the specified number of submissions, and collects up to a set number of comments
    from each submission. It ensures that duplicate submissions and comments are not collected
    by tracking their IDs. The collected data includes submission details such as title, selftext,
    score, URL, and associated comments with their respective details.

    Parameters:
        keyword (str): The keyword to search for in submissions.
        limit_per_request (int): Maximum number of submissions to retrieve per request (default is 1000).
        total_requests (int): Total number of requests to make (default is 5).
        max_comments (int): Maximum number of comments to collect per submission (default is 70).

    Returns:
        list: A list of dictionaries containing submission and comment data.
    """

    subreddit = reddit.subreddit('all')
    data = []

    last_submission = None
    seen_submission_ids = set()  # To track already collected submission IDs

    for _ in range(total_requests):
        search_results = subreddit.search(keyword, limit=limit_per_request, params={'after': last_submission})

        for submission in search_results:
            if submission.id in seen_submission_ids:
                continue 
            
            seen_submission_ids.add(submission.id)  # Mark this submission as seen

            submission_data = {
                'submission_id': submission.id,
                'title': submission.title,
                'selftext': submission.selftext,
                'created_utc': submission.created_utc,
                'author_id': str(submission.author),
                'score': submission.score,
                'url': submission.url,
                'num_comments': submission.num_comments,
                'comments': []
            }
            
            submission.comments.replace_more(limit=0)
            seen_comment_ids = set()  # To track already collected comment IDs
            comment_count = 0 
            
            for comment in submission.comments.list():
                if comment_count >= max_comments:
                    break  
                
                if comment.id in seen_comment_ids:
                    continue  
                
                seen_comment_ids.add(comment.id)  # Mark this comment as seen
                
                comment_data = {
                    'comment_id': comment.id,
                    'body': comment.body,
                    'created_utc': comment.created_utc,
                    'author_id': str(comment.author),
                    'score': comment.score,
                    'link_id': comment.link_id,
                    'parent_id': comment.parent_id
                }
                submission_data['comments'].append(comment_data)
                comment_count += 1  
            
            data.append(submission_data)
            last_submission = submission.id  # Update last_submission for pagination

    return data

def create_posts_df(data):
    """
    Function for saving the posts for specific keyword in a dataframe. 
    Cleaning and processing it for further analysis
    """
    posts = []
    for i in range(len(data)):
        post = {k: v for k, v in data[i].items() if k != "comments"}
        posts.append(post)

    posts_df = pd.DataFrame(posts)
    posts_df['Created'] = pd.to_datetime(posts_df['created_utc'], unit='s')

    df_posts = posts_df.drop_duplicates(subset='title')
    
    return df_posts

def create_comments_df(data, keyword):
    """
    Function for saving the comments for specific keyword in a dataframe. 
    Cleaning and processing it for further analysis
    """
    
    all_comments = []
    for i in range(len(data)):
        comments_list = data[i]["comments"]
        for comment in comments_list:
            comment["post_id"] = data[i]["submission_id"]
            all_comments.append(comment)

    # Creating a dataframe of comments
    comments_df = pd.DataFrame(all_comments)
    comments_df['Created'] = pd.to_datetime(comments_df['created_utc'], unit='s')
    comments_df = comments_df[['post_id', 'Created', 'comment_id', 'body', 'created_utc', 'author_id', 'score', 'link_id', 'parent_id']]
    comments_df = comments_df.drop_duplicates(subset = "body")
    comments_df["Keyword"] = keyword
    
    return comments_df

def merge_comments_df(df1, df2, df3):
    """
    Function for merging and connecting all the dataframes with different keywords
    """
    merged_df = pd.concat([df1, df2, df3], ignore_index=True)
    merged_df = merged_df.drop_duplicates(subset = "body")
    
    return merged_df

# Saving all the comments for 3 keywords scraped

keywords = ["Medical AI",
            "Trust in AI diagnostics",
            "AI in medicine"]

data1 = collect_data(keywords[0], limit_per_request = 1000, total_requests = 5, max_comments = 70)
data2 = collect_data(keywords[1], limit_per_request = 1000, total_requests = 5, max_comments = 50)
data3 = collect_data(keywords[2], limit_per_request = 1000, total_requests = 5, max_comments = 50)

comments_df1 = create_comments_df(data1, keywords[0])
comments_df2 = create_comments_df(data2, keywords[1])
comments_df3 = create_comments_df(data3, keywords[2])

all_comments = merge_comments_df(comments_df1, comments_df2, comments_df3)
all_comments.to_csv("all_comments.csv")
