from controller import scraper

max_period = 18
for i in range(1, max_period + 1):
    scraper.scrape_period(i, max_period)
