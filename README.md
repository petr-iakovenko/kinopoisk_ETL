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