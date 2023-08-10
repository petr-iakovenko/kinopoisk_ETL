# kinopoisk_ETL

## Steps for ET 

### 1. Create and run in docker new database - postgres

1. Use run docker-compose `docker-compose up -d`
2. Container check `docker ps -a `  
Processed data will be uploaded to this database  

### 2. Use Jupyter Notebook for write ET(extract, load) code.

1. To get data from kinopoisk use library `pip install kinopoisk-dev`.
2. Connecting to API Kinopoisk for get data. 

```python
from kinopoisk_dev import KinopoiskDev, MovieParams, MovieField

kp = KinopoiskDev(token='ZVEPHXS-QHPMTZ7-NC8ED8G-?????')
item = kp.find_many_movie(
        params=[
            MovieParams(keys=MovieField.PAGE, value=1),
            MovieParams(keys=MovieField.LIMIT, value=250),
        ]
    )
```

3. Transforming data from last step(2.) to DataFrame 'df_main_movies'.

```python
import pandas as pd
df = pd.DataFrame(item) # to dataframe
df_main_movies = df.at[4, 1]
df_main_movies = pd.DataFrame(df_main_movies)
```

4. Checking data in DataFrame 'df_main_movies'.

```python
df_main_movies
```
Get 250 rows × 39 columns
![df_main_movies](https://github.com/petr-iakovenko/kinopoisk_ETL/blob/main/screenshot_df_main_movies.png)

5. Updating data in DataFrame 'df_main_movies'. 

```python
def rename_columns_df_main_movies(film_stg):
    """return DF with renamed names columns"""
    columns = [x[0] for x in df_main_movies.values[0]]
    df_main_movies.columns = columns
    return df_main_movies


def update_rows_df_main_movies(film_stg):
    """ return DF with updated rows"""
    amount_columns = df_main_movies.columns
    columns = [x[0] for x in df_main_movies.values[0]]
    for i in range(len(amount_columns)): # iterating by number of columns
        data = [x[i][1] for x in df_main_movies.values] # bypassing the column names in data rows
        new_column = pd.Series(data, name=columns[i]) # create Series for update "df_main_movies"
        df_main_movies.update(new_column)
    return df_main_movies


def del_trash_df_main_movies(df_main_movies):
    """return DF with deleted "trash" columns"""
    list_trash = ['names', 'typeNumber', 'description', 'shortDescription', 'slogan', 'status', 'votes', 'ratingMpaa', 'ageRating', 'logo',
    'poster', 'backdrop', 'videos', 'persons', 'reviewInfo', 'seasonsInfo', 'budget', 'fees', 'premiere', 'similarMovies',
    'sequelsAndPrequels', 'watchability', 'top10', 'top250', 'enName', 'facts', 'imagesInfo', 'productionCompanies',]
    for i in list_trash:
        df_main_movies.drop(i, axis= 1 , inplace= True)
    return df_main_movies

df_main_movies = rename_columns_df_main_movies(df_main_movies)
df_main_movies = update_rows_df_main_movies(df_main_movies)
df_main_movies = del_trash_df_main_movies(df_main_movies)
```

6. Checking data in DataFrame 'df_main_movies'.

```python
df_main_movies
```
Get 250 rows × 11 columns
![df_main_movies](https://github.com/petr-iakovenko/kinopoisk_ETL/blob/main/6.%20To%20check%20data.png)

7. Transforming column 'releaseYears' in DataFrame 'df_main_movies'.
```python
releaseYears = df_main_movies['releaseYears'] # Series
col = len(releaseYears)
for n in range(col):
    object_ = releaseYears.loc[n]
    if object_ is None:
        pass
    elif len(object_) == 0:
         releaseYears.loc[n] = None
    else:
        for i in range(len(releaseYears.loc[n])):
            first = str(object_[i]).find("(")
            j = str(object_[i])[first+1:]
            j = j.split()
            releaseYears.loc[n] = j
```

8. Creating function for transform rows. 
```python
def names_rows(name_col, name_df):
    """return list for update dataframe"""
    full_list_rows_df = name_df[name_col].to_list() # to list full data from "name_df" for transform
    final_list_rows_df = []
    for rows_df in full_list_rows_df:
        new_list_rows_df_1 = (str(rows_df)).split(' ')
        for items_rows_df in new_list_rows_df_1:
            new_list_rows_df_2 = items_rows_df.split('=')
            for index in range(len(new_list_rows_df_2)):
                new_list_rows_df_2[index] = new_list_rows_df_2[index].replace("'",'')
            final_list_rows_df.append(new_list_rows_df_2)
    return final_list_rows_df
```

9. Creating and updating table 'externalId' in DataFrame 'df_externalId'
```python
def create_and_upgrade_df_externalId(raw_names_list):
    """return df_externalId with new columns and rows"""
    names_rows_list = [x for x in raw_names_list] # give list with names rows
    """ names_rows_list = [['kpHD', '4127663ed234fa8584aeb969ceb02cd8'], ['imdb', 'tt1675434'], ['tmdb', '77338'],......]"""
    names_columns_list = [x[0] for x in raw_names_list][0:3] # give list with names table "['kpHD', 'imdb', 'tmdb']"
    items_kpHD_list_ = []
    items_imdb_list_ = []
    items_tmdb_list_ = []
    for items in names_rows_list:
      if items[0] == names_columns_list[0]:
          items_kpHD_list_.append(items[1])
      elif items[0] == names_columns_list[1]:
          items_imdb_list_.append(items[1])
      elif items[0] == names_columns_list[2]:
          items_tmdb_list_.append(items[1])
    df_externalId.insert(loc=1, column=names_columns_list[0], value=items_kpHD_list_) # insert value rows in columns for DF "externalId"
    df_externalId.insert(loc=1, column=names_columns_list[1], value=items_imdb_list_)
    df_externalId.insert(loc=1, column=names_columns_list[2], value=items_tmdb_list_)
    del df_externalId[df_externalId.columns [0]]
    return df_externalId


df_externalId = df_main_movies.pop('externalId') # execute column "externalId" from "film_stg" for create new table "df_externalId"
df_externalId = pd.DataFrame(df_externalId)
raw_names_list = names_rows('externalId', df_externalId) # call def names_rows() for transform raw data from "df_externalId" and append into list "names"

df_externalId = create_and_upgrade_df_externalId(raw_names_list)
```
10. Checking data in DataFrame 'df_externalId'.

```python
df_externalId
```

Get 250 rows × 3 columns
![df_externalId](https://github.com/petr-iakovenko/kinopoisk_ETL/blob/main/10.%20Check%20data%20df_externalId.png)

11. Creating function for transform DataFrame "df_rating".
```python
def create_and_upgrade_df_rating(raw_names_list):
    """return df_rating with new columns and rows"""
    names_rows_list = [x for x in raw_names_list] # give list with names rows
    """ names_rows_list = [['kpHD', '4127663ed234fa8584aeb969ceb02cd8'], ['imdb', 'tt1675434'], ['tmdb', '77338'],......]"""
    names_columns_list = [x[0] for x in raw_names_list][0:6] # give list with names table "['kp', 'imdb', 'tmdb', 'filmCritics', 'russianFilmCritics', 'await_']"
    items_kp_list_ = []
    items_imdb_list_ = []
    items_tmdb_list_ = []
    items_filmCritics_list_ = []
    items_russianFilmCritics_list_ = []
    items_await_list_ = []
    for items in names_rows_list:
      if items[0] == names_columns_list[0]:
          items_kp_list_.append(items[1])
      elif items[0] == names_columns_list[1]:
          items_imdb_list_.append(items[1])
      elif items[0] == names_columns_list[2]:
          items_tmdb_list_.append(items[1])
      elif items[0] == names_columns_list[3]:
          items_filmCritics_list_.append(items[1])
      elif items[0] == names_columns_list[4]:
          items_russianFilmCritics_list_.append(items[1])
      elif items[0] == names_columns_list[5]:
          items_await_list_.append(items[1])
    df_rating.insert(loc=1, column=names_columns_list[0], value=items_kp_list_) # insert value rows in columns for DF "externalId"
    df_rating.insert(loc=1, column=names_columns_list[1], value=items_imdb_list_)
    df_rating.insert(loc=1, column=names_columns_list[2], value=items_tmdb_list_)
    df_rating.insert(loc=1, column=names_columns_list[3], value=items_filmCritics_list_)
    df_rating.insert(loc=1, column=names_columns_list[4], value=items_russianFilmCritics_list_)
    df_rating.insert(loc=1, column=names_columns_list[5], value=items_await_list_)
    del df_rating[df_rating.columns [0]]
    return df_rating


df_rating = df_main_movies.pop('rating') # execute column "rating" from "film_stg" for create new table "df_rating"
df_rating = pd.DataFrame(df_rating) # Series to DF for next stroke
raw_names_list = names_rows('rating', df_rating) # call def names_rows() for transform raw data from "df_rating" and append into list "names"
df_rating = create_and_upgrade_df_rating(raw_names_list)
```

12. Checking data in DataFrame 'df_rating'.

```python
df_rating
```

Get 250 rows × 6 columns
![df_rating](https://github.com/petr-iakovenko/kinopoisk_ETL/blob/main/12.%20Check%20data%20df_reting.png)

13. Creating function for transform data and create DataFrame.

```python
def names_list(name_col):
    """ return DF where in data deleted "'" """
    df_name_col = df_main_movies.pop(name_col)
    df_name_col = pd.DataFrame(df_name_col)
    col = range(len(df_name_col))
    for index in col:
        object_ = df_name_col.loc[index]
        object_ = object_.tolist()
        for i in range(len(object_[0])):
            first_simbol = str(object_[0][i]).find("'")
            last_simbol  = str(object_[0][i]).rfind("'")
            j = str(object_[0][i])[first_simbol +1:last_simbol ]
            object_[0][i] = j
    return df_name_col
```

14. Use func 'names_list()' for transform data and creating DataFrame 'df_genres'. Checking data in created DataFrame 'df_genres'.

```python
df_genres = names_list('genres') # use func for transform data and create DataFrame df_genres
df_genres
```
250 rows × 1 columns    
![df_genres](https://github.com/petr-iakovenko/kinopoisk_ETL/blob/main/14.%20Check%20data%20df_genres.png)

15. Use func 'names_list()' for transform data and creating DataFrame 'df_countries'. Checking data in created DataFrame 'df_countries'.

```python
df_countries = names_list('countries') # use func for transform data and create DataFrame df_countries
df_countries
```
250 rows × 1 columns    
![df_countries](https://github.com/petr-iakovenko/kinopoisk_ETL/blob/main/15.%20Check%20data%20df_countries.png)

16. Checking data in main DataFrame 'df_main_movies'.

```python
df_main_movies # checking data in main DataFrame df_main_movies
```

250 rows × 7 columns      
![df_main_movies](https://github.com/petr-iakovenko/kinopoisk_ETL/blob/main/16.%20Check%20data%20df_main_movies.png)

For next step I will can using save data from DataFrame to csv. 

```python
df_externalId.to_csv('externalId.csv')
df_rating.to_csv('df_rating.csv')
df_genres.to_csv('genres.csv')
df_countries.to_csv('countries.csv')
df_main_movies.to_csv('stg.csv')
```

and load csv-files to some database