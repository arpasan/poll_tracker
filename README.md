# Poll Tracker
This repo contains a poll tracker that scrapes The Economist's aggregate [polling page](https://cdn-dev.economistdatateam.com/jobs/pds/code-test/index.html), and outputs two CSVs:
```diff
- polls.csv
+ trends.csv
```
The former with polling figures, and the latter with polling averages.


* [`JoinOutputCSV`](https://github.com/arpasan/poll_tracker/blob/29c3460bb35219ec2118f759780df5fcee91dec4/joinoutputcsv.py)
> Class that generates the respective CSVs. It appends a timestamp to the filename, so that the next time that the polling page gets scraped, it checks against locally saved files for the base name and performs an outer join on any previous `polls` or `trends` CSVs, and dedupes. The new file will therefore contain all the previously saved information as well as the new one. This comes in handy should any of the old polling data be altered or removed when the polling page gets updated with the latest polls *after* one's initial scrape. (Such as, for instance, if some candidates get squeezed out of the polls, so they no longer occupy a separate column, but get relegated amongst `'Other'`.)
>> Contains functions controlling the outer join and checks' logic.
>>> **NB** This assumes that, given `n` consecutive scrapes where $n \in \mathbb{N}_{\geq 2}$, the user deletes the old files, keeping only the latest files, so that no more than one `polls` and one `trends` CSV file exists locally at the time of a new scrape.

* [`PollsWebScraper`](https://github.com/arpasan/poll_tracker/blob/29c3460bb35219ec2118f759780df5fcee91dec4/pollswebscraper.py)
> Class that scrapes the polling page, and throws an error should the request fail, or if an error occurs during the parsing stage. The user has a choice to perform a `'Simple'` average, whereby each candidate gets a calculated average for their polls for a given date if such data exists across the pollsters, or `'Rolling'` average, which is a seven-day moving average for each candidate given sufficient data.
>> Contains functions doing the formatting, scraping, and averaging operations.

* [`main.py`](https://github.com/arpasan/poll_tracker/blob/29c3460bb35219ec2118f759780df5fcee91dec4/main.py)
> Runs the entire process. `'Rolling'` average and the polling page's URL are passed as arguments by default.

Should you wish to change the rolling average to a simple polling mean, you may want to download this repo, and modify the `main.py` file accordingly. For further details, read the `.py`s' annotations; they are richly commented and self-explanatory.

(NB This is an assignment solution submitted by Andrej Arpáš.)
