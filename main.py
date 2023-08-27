# import requests

from pollswebscraper import PollsWebScraper
from joinoutputcsv import JoinOutputCSV

poll_tracker = JoinOutputCSV()

poll_tracker.polls_trends(url='https://cdn-dev.economistdatateam.com/jobs/pds/code-test/index.html', average_type='Rolling')
