# Climate_News_Scraper_ETL
Using this project one is capable of scraping the [BBC News](https://www.bbc.com/news/science-environment-56837908) website for the latest updates on climate. The scraper functionality is packed in an ETL pipeline build on Prefect and Dask in order to load the scraped news articles in a SQL Lite database.

### Webscraping
For webscraping the libraries *requests* and *beautifulsoup* are used. Only the latest articles can be scraped, therefore the script is intended to run on a periodic schedule. 

### ETL
*Prefect* is used for orchestration of the ETL flow. The flow an easily be monitored from the [Prefect Cloud](https://www.prefect.io/cloud/) platform. 
![image](https://user-images.githubusercontent.com/39828550/215114016-a70d17af-d7d7-4969-864b-affe0ad73a9b.png)


### Sentiment classification
Using the TextBlob library the sentiment (negative/neutral/positive) is added to each article. 

### Database
All scraped articles will be written to a local [SQLite](https://sqlite.org/index.html) database in the load stage of the ETL flow. No duplicate entries are allowed. Here is an example of a record inserted into the CLIMATENEWS table.
```
{ 
  "title" : "Warning climate change impacting on avalanche risk", 
  "content" : "Forecasters said a likely effect in Scotland was avalanches occurring in tighter periods of time.", 
  "date" : "2023-01-27T06:04:00.000000000", 
  "sentiment" : "Negative" 
}
```
