# kinopoisk_ETL

## Steps for ET 

### 1. Create and run in docker new database - postgres

1. Use run docker-compose `docker-compose up -d`
2. Container check `docker ps -a `  
Processed data will be uploaded to this database  

### 2. Use Jupyter Notebook for write ET(extract, load) code.

1. To get data from kinopoisk use library `pip install kinopoisk-dev`.
2. Connecting to API Kinopoisk for get data  

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

3. Transform data from kinopoisk to DataFrame.

```python
import pandas as pd
df = pd.DataFrame(item) # to dataframe
df_main_movies = df.at[4, 1]
df_main_movies = pd.DataFrame(df_main_movies)
```

4. To check data.

```python
df_main_movies
```
Get 250 rows × 39 columns
![df_main_movies](https://github.com/petr-iakovenko/kinopoisk_ETL/blob/main/screenshot_df_main_movies.png)

5. Update df_main_movies 

```python
def rename_columns_df_main_movies(film_stg):
    """return DF with renamed names columns"""
    columns = [x[0] for x in df_main_movies.values[0]]
    df_main_movies.columns = columns
    return df_main_movies


def update_rows_df_main_movies(film_stg):
    """ return DF with updated rows"""
     # give amount of columns
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

6. To check data.

```python
df_main_movies
```
Get 250 rows × 11 columns
![df_main_movies](https://github.com/petr-iakovenko/kinopoisk_ETL/blob/main/6.%20To%20check%20data.png)
