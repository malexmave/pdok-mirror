# -*- encoding: utf-8 -*-
"""Scraper for the pdoc file structures.

Copyright (C) 2017  Max Maass

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published
by the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

from util.util import _download_tuple, get_html, pdf_to_text
from models.database import Wahlperiode, Plenarprotokoll, Drucksache
from multiprocessing.pool import ThreadPool, Process
from peewee import DoesNotExist
from sys import stdout
import re
import os


DOWNLOAD_WORKERS = 3
INSERT_WORKERS = 2

BASEURL_META_PLENARY = "http://pdok.bundestag.de/treffer.php?q={}&wp=&dart=Plenarprotokoll"
BASEURL_META_DRUCKSACHE = "http://pdok.bundestag.de/treffer.php?q={}&wp=&dart=Drucksache"
BASEURL_DOC_PLENARY = "http://dipbt.bundestag.de/doc/btp/{0}/{0}{1}.pdf"
BASEURL_DOC_DRUCKSACHE = "http://dipbt.bundestag.de/doc/btd/{0}/{1}/{0}{2}.pdf"
BASEPATH_FILE_PLENARY = "documents/{0}/Plenarprotokoll/{0}{1}.pdf"
BASEPATH_FILE_DRUCKSACHE = "documents/{0}/Drucksache/{0}{1}.pdf"


def scrape_period(period_no_numeric, max_period):
    """Scrape all data for a specific election period.

    Arguments:
    period_no_numeric -- The number of the period.
    """
    # Format period number properly
    period_no = '%02d' % period_no_numeric
    # Get from database or create
    period = Wahlperiode.get_or_create(period_no=period_no)[0]

    # If the period has already been scraped, return instantly
    if period.period_scraped:
        print "INFO: Period", period_no, "has already been scraped - skipping"
        return

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

    # Check if we have processed an old period, and if yes, mark it as
    # completely downloaded (in the hopes that all downloaded files are in
    # order, and none of them failed)
    # TODO Maybe add a check that all files are indeed application/pdf files
    #      before marking the period as completed, just to be sure?
    if period_no_numeric < max_period:
        print "INFO: Marking period", period_no, "as completely scraped."
        period.period_scraped = True

    # Save changes to the period database entry
    period.save()


def scrape_period_plenarprotokoll(period):
    """Scrape the website for Plenarprotokolle in the given period.

    Arguments:
    period -- A models.database.Wahlperiode object
    """
    pool = ThreadPool(processes=DOWNLOAD_WORKERS)
    workqueue = []
    for number in range(1, 1000):
        # We do not start from the highest already scraped plenary because
        # the download code also checks if the file was successfully downloaded
        # as a PDF file.  Thus, if any error sneaks through on one pass, e.g.
        # because the server is unavailable for a moment, it will get fixed
        # automatically on the next pass.
        # As the metadata remains unchanged, we don't need to reprocess these
        # files in the database.
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


def scrape_period_drucksachen(period):
    """Scrape the website for Drucksachen in the given period.

    Arguments:
    period -- A models.database.Wahlperiode object
    """
    pool = ThreadPool(processes=DOWNLOAD_WORKERS)
    workqueue = []
    for number in range(0 + 1, 20000):
        # We do not start from the highest already scraped Drucksache because
        # the download code also checks if the file was successfully downloaded
        # as a PDF file.  Thus, if any error sneaks through on one pass, e.g.
        # because the server is unavailable for a moment, it will get fixed
        # automatically on the next pass.
        # As the metadata remains unchanged, we don't need to reprocess these
        # files in the database.
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
    period_part = filename[:2]
    number_part = int(filename[2:])
    docno = period_part + "/" + str(number_part)

    # Check if we already processed this
    if period.plenary_max >= number_part:
        return

    # Check if database entry already exists
    try:
        Plenarprotokoll.get(docno=docno)
        period.plenary_max = number_part
        # print "WARN: Database entry for plenary", docno, "already exists. Skipping"
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
    Plenarprotokoll.create(docno=docno, date=date, path=path,
                           period=period, title=title, source=source)

    # Update maximum processed number, modulo special cases (which are always
    # above 399, as experience shows).  This allows us to later skip already
    # processed documents more efficiently (compared to querying the database
    # for each document, which is quite a drag on performance using SQLite)
    if number_part < 399:
        period.plenary_max = number_part


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
    number_part = int(filename[2:])
    docno = filename[:2] + "/" + str(number_part)

    # Skip already processed documents
    if number_part <= period.drucksache_max:
        return
    # Check if database entry already exists
    try:
        Drucksache.get(docno=docno)
        # print "WARN: Database entry for Drucksache", docno, "already exists. Skipping"
        period.drucksache_max = number_part
        period.save()
        return
    except DoesNotExist:
        pass

    # Retrieve metadata on entry
    metadata = scrape_drucksache_meta(docno)
    if metadata is not None:
        title, date, doctype, urheber, autor = metadata
    else:
        print "ERROR: No metadata found for Drucksache", docno, "- skipping"
        return
    if date is None:
        print "ERROR: Scraping plenary meta appears to have failed for docno", docno
        return

    # Create new database entry
    source = BASEURL_DOC_DRUCKSACHE.format(filename[:2], filename[2:5], filename[2:])
    Drucksache.create(docno=docno, date=date, path=path, period=period,
                      title=title, doctype=doctype, urheber=urheber,
                      autor=autor, source=source)

    # Update maximum processed Drucksachen-number
    period.drucksache_max = number_part


def scrape_plenarprotokoll_meta(docno):
    """Scrape metadata for a Plenarprotokoll from the PDOK server.

    Arguments:
    docno -- The document number in the canonical format (e.g. 13/37, but not 13/037)

    Returns a 2-tuple of metadata: (title, date)
    """
    res = check_hardcoded_cornercases_plenary(docno)
    if res is not None:
        return res
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

    Returns a 5-tuple of metadata: (title, date, doctype, urheber, autor)
    """
    # Some documents have weird cornercases, which have been solved through
    # hardcoded results.  This is not pretty, but preferrable to the
    # alternative of not having metadata for them
    res = check_hardcoded_cornercases_drucksache(docno)
    if res is not None:
        return res

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


def check_hardcoded_cornercases_drucksache(docno):
    """Check if the document number is a weird corner case.

    Some documents aren't findable properly using the PDOK system. This can be
    from a number of reasons, but the result is that we have to manually add
    them to our database. To make this easier, these cornercases are handled
    explicitly in the code, as far as they are known at the time of writing.
    If you encounter any additional corner cases, send a pull request with the
    updated information :).

    Arguments:
    docno -- the document number to check for corner cases
    """
    cornercases = {
        # Order in tuple: (title, date, doctype, urheber, autor)
        "06/10001": ("Zwischenbericht über den Vollzug des Gesetzes zum Schutz gegen Fluglärm vom 30. März 1972 (BGBl. I S. 282) Bezug: Beschluß des Deutschen Bundestages vom 16. Dezember 1970 - Drucksache VI/1377 - ",
                     "20.10.1972", "Unterrichtung", "Bundesministerium des Innern", None),
        "06/10002": ("Bericht des Parlamentarischen Staatssekretärs Heinz Westphal im Bundesministerium für Jugend, Familie und Gesundheit über Rauschmittelmißbrauch",
                     "31.10.1972", "Unterrichtung", "Bundesministerium für Jugend, Familie und Gesundheit", None),
        "06/10003": ("Bericht des Bundesministers des Innern über Rauschmittelmißbrauch",
                     "06.11.1972", "Unterrichtung", "Bundesministerium des Innern", None),
    }
    try:
        return cornercases[docno]
    except:
        return None


def check_hardcoded_cornercases_plenary(docno):
    """Check if the document number is a weird corner case.

    Some documents aren't findable properly using the PDOK system. This can be
    from a number of reasons, but the result is that we have to manually add
    them to our database. To make this easier, these cornercases are handled
    explicitly in the code, as far as they are known at the time of writing.
    If you encounter any additional corner cases, send a pull request with the
    updated information :).

    Arguments:
    docno -- the document number to check for corner cases
    """
    cornercases = {
        # Order in tuple: (title, date)
        "16/300": ("13. Bundesversammlung der Bundesrepublik Deutschland", "23.05.2009"),
        "17/500": ("15. Bundesversammlung der Bundesrepublik Deutschland", "18.03.2012"),
        "17/907": ("15. Bundesversammlung der Bundesrepublik Deutschland", "18.03.2012"),
        "18/909": ("16. Bundesversammlung der Bundesrepublik Deutschland", "12.02.2017"),

    }
    try:
        return cornercases[docno]
    except:
        return None
