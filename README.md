# Climate_News_Scraper_ETL
Using this project one is capable of scraping the [BBC News](https://www.bbc.com/news/science-environment-56837908) website for the latest updates on climate. The scraper functionality is packed in an ETL pipeline build on Prefect and Dask in order to load the scraped news articles in a SQL Lite database.

### Sentiment classification
Using the TextBlob library the sentiment (negative/neutral/positive) is added to each article. 

### Database
All scraped articles will be written to a local SQLite database in the load stage of the ETL flow. No duplicate entries are allowed. Here is the output of a select query on the CLIMATENEWS table 

'''SELECT * FROM CLIMATENEWS'''
