
from prefect import flow, task
import dask.dataframe as dd
from newsscraper import ClimateNews
from textblob import TextBlob
import pandas as pd 
import sqlite3
from sqlite3 import Error

@flow
def run_flow():
    
    # extract
    df = scrape_news()

    # transform
    df = classify_sentiment(df)

    # load
    load_news(df)


@task
def scrape_news() -> dd:
    """
    Scrape climate news
    Returns : Dask.DataFrame
    """
    cns = ClimateNews()
    df = cns.scrape()
    return df

@task
def classify_sentiment(df:dd) -> dd:

    all_content = df['content'].compute().tolist()

    #Create a function to get the polarity

    def get_sentiment(text):
        score = TextBlob(text).sentiment.polarity
        if score < 0:
            return "Negative"
        elif score == 0:
            return "Neutral"
        else:
            return "Positive"

    sentiment_list = []
    for text in all_content:
        sentiment_list.append(get_sentiment(text))

    df["sentiment"] = pd.Series(sentiment_list)

    return df

@task
def load_news(df) -> None:
    
    # Connection string
    conn = sqlite3.connect("climatenews.db")

    # Cursor object
    cursor = conn.cursor()

    # Creating table (if not exists)
    table = """ CREATE TABLE IF NOT EXISTS CLIMATENEWS (
                    title VARCHAR(255) NOT NULL,
                    content VARCHAR(255) NOT NULL,
                    date DATETIME() NOT NULL,
                    sentiment VARCHAR(8) NOT NULL
            ); """
    cursor.execute(table)
    
    # Insert data
    insert_query = ''' INSERT OR IGNORE INTO CLIMATENEWS(title,content,date,sentiment)
              VALUES {}'''.format(df.compute().to_records(index=False))
    cursor.execute(insert_query)
    
    # Commit the transaction
    conn.commit()
    
    # Check
    # print(pd.read_sql("SELECT * FROM CLIMATENEWS, conn))
    
    # Close the connection
    conn.close()
  

if __name__ == "__main__":
    run_flow()
