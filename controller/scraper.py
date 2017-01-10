# -*- encoding: utf-8 -*-
"""Scraper for the pdoc file structures."""

from util.util import _download_tuple, get_html, pdf_to_text
from models.database import Wahlperiode, Plenarprotokoll, Drucksache
from multiprocessing.pool import ThreadPool, Process
from peewee import DoesNotExist
from sys import stdout
import re
import os


DOWNLOAD_WORKERS = 20
INSERT_WORKERS = 2

BASEURL_META_PLENARY = "http://pdok.bundestag.de/treffer.php?q={}&wp=&dart=Plenarprotokoll"
BASEURL_META_DRUCKSACHE = "http://pdok.bundestag.de/treffer.php?q={}&wp=&dart=Drucksache"
BASEURL_DOC_PLENARY = "http://dipbt.bundestag.de/doc/btp/{0}/{0}{1}.pdf"
BASEURL_DOC_DRUCKSACHE = "http://dipbt.bundestag.de/doc/btd/{0}/{1}/{0}{2}.pdf"
BASEPATH_FILE_PLENARY = "documents/{0}/Plenarprotokoll/{0}{1}.pdf"
BASEPATH_FILE_DRUCKSACHE = "documents/{0}/Drucksache/{0}{1}.pdf"


def scrape_period(period_no):
    """Scrape all data for a specific election period.

    Arguments:
    period_no -- The number of the period.
    """
    # Format period number properly
    period_no = '%02d' % period_no
    # Get from database or create
    period = Wahlperiode.get_or_create(period_no=period_no)[0]

    # Ensure directory structure exists
    if not os.path.exists('documents/' + period_no + "/Plenarprotokoll"):
        os.makedirs('documents/' + period_no + "/Plenarprotokoll")
    if not os.path.exists('documents/' + period_no + "/Drucksache"):
        os.makedirs('documents/' + period_no + "/Drucksache")

    # Scrape all Plenarprotokolle
    print "INFO: Scraping Plenarprotokolle for period", period_no
    scrape_period_plenarprotokoll(period)
    # Scrape Drucksachen
    print "INFO: Scraping Drucksachen for period", period_no
    scrape_period_drucksachen(period)


def scrape_period_plenarprotokoll(period):
    """Scrape the website for Plenarprotokolle in the given period.

    Arguments:
    period -- A models.database.Wahlperiode object
    """
    pool = ThreadPool(processes=DOWNLOAD_WORKERS)
    workqueue = []
    for number in range(period.plenary_max + 1, 1000):
        number = '%03d' % number
        url = BASEURL_DOC_PLENARY.format(period.period_no, number)
        file = BASEPATH_FILE_PLENARY.format(period.period_no, number)

        # Queue up download
        workqueue += [(url, file)]

    # TODO Add progress bars to all of this
    # Perform download
    print "INFO: Downloading Plenarprotokolle...",
    stdout.flush()
    results = pool.map(_download_tuple, workqueue)
    print "DONE."

    # Close and terminate pool
    pool.close()
    pool.join()

    # Create background process for pdftotext operations
    p = Process(target=pdf_to_text, args=(results, ), name="conv_plenary")
    p.start()

    print "INFO: Inserting to database...",
    stdout.flush()
    for result in results:
        process_plenarprotokoll(period, result)

    # Wait for conversion worker to finish
    print "DONE."
    print "INFO: Waiting for text file conversion to finish...",
    stdout.flush()
    p.join()
    print "DONE."
    # # Create a new pool for processing the finished downloads
    # pool = ThreadPool(processes=INSERT_WORKERS)
    # # Define a partial function to make calling the processing function easier
    # partial_process = partial(process_plenarprotokoll, period)

    # # Process finished downloads
    # pool.map(partial_process, results)
    # # Clean up pool
    # pool.close()
    # pool.join()


def scrape_period_drucksachen(period):
    """Scrape the website for Drucksachen in the given period.

    Arguments:
    period -- A models.database.Wahlperiode object
    """
    pool = ThreadPool(processes=DOWNLOAD_WORKERS)
    workqueue = []
    for number in range(period.drucksache_max + 1, 20000):
        number = "%05d" % number
        prefix = number[:3]

        # Prepare url and path
        url = BASEURL_DOC_DRUCKSACHE.format(period.period_no, prefix, number)
        file = BASEPATH_FILE_DRUCKSACHE.format(period.period_no, number)

        # Queue up
        workqueue += [(url, file)]

    # TODO Add progress bars
    # Perform download
    print "INFO: Downloading Drucksachen...",
    stdout.flush()
    results = pool.map(_download_tuple, workqueue)
    print "DONE."

    # Close and terminate pool
    pool.close()
    pool.join()

    # Create background process for pdftotext operations
    p = Process(target=pdf_to_text, args=(results, ), name="conv_druck")
    p.start()

    print "INFO: Inserting into database...",
    stdout.flush()
    for result in results:
        process_drucksache(period, result)
    print "DONE"
    # Wait for text file conversion to finish
    print "INFO: Waiting for text file conversion to finish...",
    stdout.flush()
    p.join()
    print "DONE."
    # Create new database worker pool
    # pool = ThreadPool(processes=INSERT_WORKERS)

    # # Define a partial function to make calling the processing function easier
    # partial_process = partial(process_drucksache, period)

    # # Process finished downloads
    # pool.map(partial_process, results)
    # # Clean up pool
    # pool.close()
    # pool.join()


def process_plenarprotokoll(period, path):
    """Process a downloaded Plenarprotokoll.

    Arguments:
    period -- A models.database.Wahlperiode the Plenarprotokoll belongs to
    path   -- The path to the downloaded file, or None if no file was downloaded
    """
    if path is None:
        return
    # Split path to get document number without .pdf
    filename = path.split("/")[3][:-4]
    # Derive canonical document number
    docno = filename[:2] + "/" + str(int(filename[2:]))

    # Check if database entry already exists
    try:
        proto = Plenarprotokoll.get(docno=docno)
        print "WARN: Database entry for plenary", docno, "already exists. Skipping"
        return
    except DoesNotExist:
        pass

    # Retrieve metadata on entry
    metadata = scrape_plenarprotokoll_meta(docno)
    if metadata is not None:
        title, date = metadata
    else:
        print "ERROR: No Metadata found for Plenarprotokoll", docno, "- skipping"
        return

    if date is None:
        print "ERROR: Scraping plenary meta appears to have failed for docno", docno
        return

    # Create new database entry
    source = BASEURL_DOC_PLENARY.format(filename[:2], filename[2:])
    proto = Plenarprotokoll.create(docno=docno, date=date, path=path,
                                   period=period, title=title, source=source)


def process_drucksache(period, path):
    """Process a downloaded Drucksache.

    Arguments:
    period -- A models.database.Wahlperiode the Drucksache belongs to
    path   -- The path to the downloaded file, or None if no file was downloaded
    """
    if path is None:
        return
    # Split path to get document number without .pdf
    filename = path.split("/")[3][:-4]
    # Derive canonical document number
    docno = filename[:2] + "/" + str(int(filename[2:]))

    # Check if database entry already exists
    try:
        proto = Drucksache.get(docno=docno)
        print "WARN: Database entry for Drucksache", docno, "already exists. Skipping"
        return
    except DoesNotExist:
        pass

    # Retrieve metadata on entry
    metadata = scrape_drucksache_meta(docno)
    if metadata is not None:
        title, date, doctype, urheber, autor = metadata
    else:
        print "ERROR: No metadata found for Drucksache", docno, "- skipping"

    if date is None:
        print "ERROR: Scraping plenary meta appears to have failed for docno", docno
        return

    # Create new database entry
    source = BASEURL_DOC_DRUCKSACHE.format(filename[:2], filename[2:5], filename[2:])
    proto = Drucksache.create(docno=docno, date=date, path=path, period=period,
                              title=title, doctype=doctype, urheber=urheber,
                              autor=autor, source=source)


def scrape_plenarprotokoll_meta(docno):
    """Scrape metadata for a Plenarprotokoll from the PDOK server.

    Arguments:
    docno -- The document number in the canonical format (e.g. 13/37, but not 13/037)

    Returns a 2-tuple of metadata: (title, date)
    """
    # Assemble URL
    url = BASEURL_META_PLENARY.format(docno)
    # Get HTML
    html = get_html(url)

    # Assemble RegEx
    meta_pat = re.compile('<strong>(.+?)</strong>')
    title_pat = re.compile('<a.*?>(.+?)</a>')
    # Match metadata pattern on HTML
    meta_results = meta_pat.findall(html)

    if meta_results is None or len(meta_results) < 3:
        print "ERROR: Parsing metadata for", docno, "failed."
        return
    if meta_results[1] != docno:
        print "ERROR: Got incorrect search result - expected docno", docno, "got", meta_results[1]
        return

    # Match title pattern on HTML
    title_results = title_pat.findall(html)

    if title_results is None or len(title_results) < 1:
        print "Error: Parsing title for", docno, "failed."
        return

    return (title_results[0], meta_results[2])


def scrape_drucksache_meta(docno):
    """Scrape metadata for a Drucksache from the PDOK server.

    Arguments:
    docno -- The document number in the canonical format (e.g. 13/37, but not 13/037)

    Returns a 2-tuple of metadata: (title, date, doctype, urheber, autor)
    """
    # Assemble URL
    url = BASEURL_META_DRUCKSACHE.format(docno)
    # Get HTML
    html = get_html(url)

    # Assemble RegEx
    meta_pat = re.compile('<strong>(.+?)</strong>')
    title_pat = re.compile('<a.*?>(.+?)</a>')
    urheber_pat = re.compile('Urheber: <strong>(.+?)</strong>')
    autoren_pat = re.compile('Autoren: (.+?)</div>')
    # Match metadata pattern on HTML
    meta_results = meta_pat.findall(html)

    if meta_results is None or len(meta_results) < 4:
        print "ERROR: Parsing metadata for", docno, "failed."
        return
    if meta_results[1] != docno:
        print "ERROR: Got incorrect search result - expected docno", docno, "got", meta_results[1]
        return

    # Match title pattern on HTML
    title_results = title_pat.findall(html)

    if title_results is None or len(title_results) < 1:
        print "Error: Parsing title for", docno, "failed."
        return

    urheber_results = urheber_pat.findall(html)

    urheber = None
    if urheber_results is not None and len(urheber_results) >= 1:
        urheber = urheber_results[0]

    autoren_results = autoren_pat.findall(html)

    autoren = None
    if autoren_results is not None and len(autoren_results) >= 1:
        autoren = autoren_results[0]

    return (title_results[0], meta_results[2], meta_results[3], urheber, autoren)
