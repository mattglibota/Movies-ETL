#import dependencies
import json, pandas as pd, numpy as np, re
from sqlalchemy import create_engine
import psycopg2
import time

def movies_challenge(wiki_data, kaggle_data, rating_data):

    def clean_movies(movie):
        movie = dict(movie) #create non-destructive copy
        alt_titles = {}
        # combine alternate titles into one list
        for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                    'Hangul','Hebrew','Hepburn','Japanese','Literally',
                    'Mandarin','McCune–Reischauer','Original title','Polish',
                    'Revised Romanization','Romanized','Russian',
                    'Simplified','Traditional','Yiddish']:
            if key in movie:
                alt_titles[key] = movie[key]
                movie.pop(key)
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles
        
        #merge column name
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)
        
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')
        
        return movie

    def parse_dollars(s):
        # if s is not a string, return NaN
        if type(s) != str:
            return np.nan
        
        # if input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " million"
            s = re.sub('\$|\s|[a-zA-Z]','',s)

            # convert to float and multiply by a million
            value = float(s) * 10**6

            # return value
            return value

        # if input is of the form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " billion"
            s = re.sub('\$|\s|[a-zA-Z]','',s)

            # convert to float and multiply by a billion
            value = float(s) * 10**9

            # return value
            return value

        # if input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

            # remove dollar sign and commas
            s = re.sub('\$|,','',s)

            # convert to float
            value = float(s)

            # return value
            return value

        # otherwise, return NaN
        else:
            return np.nan

    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(
            lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis = 1)
        df.drop(columns=wiki_column, inplace=True)

    file_dir = "C:/Users/mattg/OneDrive/Documents/Data Analytics Bootcamp/Module 8/Movies-ETL/"

### START EXTRACT AND TRANSFORM FOR WIKI DATA

    #open json for wiki movies
    with open(f'{file_dir}{wiki_data}',mode='r') as file:
        wiki_movies_raw = json.load(file)

    #removed any television shows or movies without directors w/ list comp
    wiki_movies = [movie for movie in wiki_movies_raw
              if ('Director' in movie or 'Directed by' in movie)
                  and 'imdb_link' in movie
                  and 'No. of episodes' not in movie]

    #run clean_movies function (alt titles and duplicate columns)
    cleaned_movies = [clean_movies(movie) for movie in wiki_movies]
    
    #convert to pandas DF
    wiki_movies_df = pd.DataFrame(cleaned_movies)

    #cleaning up duplicates rows by extracting imdb ID, then drop old dup
    wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
    wiki_movies_df.drop_duplicates(subset='imdb_id',inplace=True)

    #find columns with less than 10% null values, then keep those cols
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns
        if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]

    ## Parse Box Office Data from WIKI
    
    #create regex forms
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illi?on)'

    #create box office series and concat lists
    box_office = wiki_movies_df['Box office'].dropna() 
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)

    #parse hyphens out to reduce errors
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    
    #create new df series for parsed box office data, then drop old col
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})')[0].apply(parse_dollars)
    wiki_movies_df.drop('Box office', axis=1, inplace=True)

    ## Parse Budget Data from WIKI

    #create budget series
    budget = wiki_movies_df['Budget'].dropna()

    #concat list, replace hyphens, replace citations
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    budget = budget.str.replace(r'\[\d+\]\s*', '')

    #add new df series for budget parsing and drop original
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    wiki_movies_df.drop('Budget', axis=1, inplace=True)

    ## Parse Release Date data from WIKI

    #release date drop nans and combine lists
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x)==list else x)

    #create proper dateforms
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'

    #add new df for release data
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
    wiki_movies_df.drop('Release date', axis=1, inplace=True)

    ## Parse Running Data from WIKI
    
    #create new df for running time cleaning
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

    #extract the hours and mins if available, then convert to a dataframe
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)

    #add parsed running_time column and drop old col
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
    wiki_movies_df.drop('Running time', axis=1, inplace=True)

### START EXTRACT AND TRANSFORM ON KAGGLE DATA

    #import kaggle metadata
    kaggle_metadata = pd.read_csv(f'{file_dir}{kaggle_data}', low_memory=False)
    
    #remove adult videos
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False']
    
    #convert video back to true
    kaggle_metadata['video'] = 'True'
    
    #convert remaining numeric and date series
    kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
    kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
    kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])



### START EXTRACT AND TRANSFORM RATINGS DATA
    
    #create df from ratings
    ratings = pd.read_csv(f'{file_dir}{rating_data}', low_memory=False)

    #convert ratings timestamp to proper dtype
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')

    #work on cleaning ratings
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
        .rename({'userId':'count'}, axis=1) \
        .pivot(index='movieId',columns='rating',values='count')

    #format the columns to include prefix
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]

    #transform rating_counts into a df
    rating_counts = pd.DataFrame(rating_counts)

### START MERGE AND CLEANING OPERATIONS

# Competing data:
# Wiki                     Movielens                Resolution
#--------------------------------------------------------------------------
# title_wiki               title_kaggle             Drop Wiki
# running_time             runtime                  Keep Kaggle, fill zeros w Wiki
# budget_wiki              budget_kaggle            Keep Kaggle, fill zeros w Wiki
# box_office               revenue                  Keep Kaggle, fill zeros w Wiki
# release_date_wiki        release_date_kaggle      Drop Wiki
# Language                 original_language        Drop Wiki
# Production company(s)    production_companies     Drop Wiki

    #merge datasets
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id',
                    suffixes=['_wiki','_kaggle'])

    # drop duplicate columns per the plan
    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)
    
    movies_df.head()

    try:
        # execute missing values function
        fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
        fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
        fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
    except NameError:
        pass

    #filter out unwanted columns
    movies_df = movies_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                       'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                       'genres','original_language','overview','spoken_languages','Country',
                       'production_companies','production_countries','Distributor',
                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                      ]]

    #rename columns
    movies_df.rename({'id':'kaggle_id',
                  'title_kaggle':'title',
                  'url':'wikipedia_url',
                  'budget_kaggle':'budget',
                  'release_date_kaggle':'release_date',
                  'Country':'country',
                  'Distributor':'distributor',
                  'Producer(s)':'producers',
                  'Director':'director',
                  'Starring':'starring',
                  'Cinematography':'cinematography',
                  'Editor(s)':'editors',
                  'Writer(s)':'writers',
                  'Composer(s)':'composers',
                  'Based on':'based_on'
                 }, axis='columns', inplace=True)

    #merge movies with ratings
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')

    #fill in movies with no ratings
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)

### START SQL LOAD TO POSTGRES

    #start of sql load
    from config import db_password
    #create connection string
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
    #create the sql engine
    engine = create_engine(db_string)

    ## TRIAL IMPORT
    #import the movie data

    sql = 'DROP TABLE IF EXISTS movies_w_ratings'
    engine.connect().execute(sql)

    movies_with_ratings_df.to_sql(name='movies_w_ratings', con=engine)