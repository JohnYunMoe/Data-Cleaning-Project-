import requests
import json
from bs4 import BeautifulSoup
import pandas as pd
import time
import random

MOVIE_API_KEY = "API_KEY"

def get_movies():
    url = f'https://api.watchmode.com/v1/list-titles/?apiKey={MOVIE_API_KEY}&source_ids=203,57'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data['titles'][:10]  # Limit to the first 10 movies
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return []

def get_streaming_services(movie_title):
    query = movie_title.replace(" ", "-").lower()
    search_url = f'https://www.justwatch.com/us/movie/{query}'
    headers = {
        'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15"
    }

    attempts = 0
    while attempts < 5:  # Limit to 5 attempts
        response = requests.get(search_url, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            services = soup.find_all('img', class_='offer__icon')
            if not services:
                print(f"No streaming services found for '{movie_title}'")
                return None
            
            # find the right html tag for the subscription services
            streaming_services = [service['alt'].strip() for service in services]
            return streaming_services
        
        elif response.status_code == 429:
            print(f"Too many requests for '{movie_title}'. Retrying in a few seconds...")
            time.sleep(random.uniform(5, 10))  # Wait for 5-10 seconds before retrying
            attempts += 1
            
        else:
            print(f"Error: Unable to reach JustWatch. Status Code: {response.status_code}")
            return []

    print(f"Failed to fetch streaming services for '{movie_title}' after multiple attempts.")
    return []

def clean_dataframe(df):
     # Remove any rows where 'title' or 'streaming_services' are NaN (missing values)
    df.dropna(subset=['title', 'streaming_services'], inplace=True)
     # Remove duplicate rows based on the 'title' column
    df.drop_duplicates(subset=['title'], inplace=True)
    df['title'] = df['title'].str.strip()
    # Format the 'streaming_services' column: if it's a list, join elements with a comma; 
    # otherwise, keep the value as is
    df['streaming_services'] = df['streaming_services'].apply(
        lambda x: ', '.join(x) if isinstance(x, list) else x
    )
     # Sort the DataFrame by 'year' and then 'title'
    df.sort_values(by=['year', 'title'], inplace=True)
    # Select only the relevant columns for the final DataFrame
    df = df[['title', 'year', 'imdb_id', 'streaming_services']]
    return df

def make_database(movies):
    movie_list = []
    for movie in movies:
        title = movie['title']
        year = movie['year']
        imdb_id = movie.get('imdb_id')
        print(f"Fetching streaming availability for '{title}'...")

        services = get_streaming_services(title)
        movie_list.append({
            'title': title,
            'year': year,
            'imdb_id': imdb_id,
            'streaming_services': services if services else "Not Available"
        })
        # random delay between requests to avoid throttling the server
        time.sleep(random.uniform(2, 5))  

    movie_df = pd.DataFrame(movie_list)
    movie_df = clean_dataframe(movie_df)
    return movie_df

def main():
    movies = get_movies()
    if not movies:
        return

    movie_df = make_database(movies)
    movie_df.to_csv('movie_database_cleaned.csv', index=False)
    print(movie_df.head())

if __name__ == "__main__":
    main()
