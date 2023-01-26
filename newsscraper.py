import dask.dataframe as dd
import requests
from bs4 import BeautifulSoup
from dateparser import parse

class ClimateNews:
    """
    # ClimateNews

    Climate news webscraper scraping news on climate from BBC online. 
    
    Parameters
    ---------------------
    max_articles : int, default=-1
        Maximum articles to be scraped. If value is set to -1, there is no limit. All available articles will be scraped. 

    npartitions : int, default=1
        Number of Pandas dataframes that compose a single Dask dataframe.

    Methods
    ---------------------
    scrape
        Method to scrape the climate articles. Returns a dataframe containing news articles. 
    """
    def __init__(self, max_articles:int=-1, npartitions:int=1):
        self.max_articles = max_articles
        self.npartitions = npartitions

    def scrape(self):
        """
        # Scrape
        Method to scrape the climate articles. 

        Returns
        -----------------
        dd.Dataframe containing news articles.
        """

        URL = "https://www.bbc.com/news/science-environment-56837908"
        page = requests.get(URL)

        soup = BeautifulSoup(page.content, "html.parser")

        # scrape attributes from articles
        title_list, content_list, date_list = [], [], []
        for article in soup.find_all("article"):
            try:
                title = article.find('header').getText(strip=True)
                content = article.find('p', class_='lx-stream-related-story--summary qa-story-summary').getText(strip=True)
                date = article.find('span', class_='qa-post-auto-meta').getText(strip=True)

                title_list.append(title)
                content_list.append(content)
                date_list.append(parse(date))

            except Exception as e:
                print("Article could not be scraped. ", e)

        # create Dask dataframe using a dictionary
        news_dict = {"title":title_list, "content":content_list, "date":date_list}
        df = dd.from_dict(news_dict, npartitions=self.npartitions)

        return df
        
