
from prefect import flow, task
import dask.dataframe as dd
from newsscraper import ClimateNews
from textblob import TextBlob
import pandas as pd 

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
    # TODO: load dataframe to sql lite database
    print(df.compute())

if __name__ == "__main__":
    run_flow()
