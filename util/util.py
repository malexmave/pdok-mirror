# -*- encoding: utf-8 -*-
"""Utility functions for downloading files, ..."""

import requests
import os.path
import subprocess


def download(url, filename, session=None):
    """Download a file, if it exists.

    Arguments:
    url      -- the url to download
    filename -- the filename to which the file should be saved
    session  -- a requests.Session-Object to use (to persist connections)
    """
    # Check if file already exists
    if os.path.isfile(filename):
        return None

    # Start downloading the file in streaming mode, to save memory
    if session is None:
        # No session passed, create new connection
        req = requests.get(url, stream=True)
    else:
        # Reuse old session
        req = session.get(url, stream=True)

    # Check if the file actually exists
    if req.status_code == 404:
        return None

    # Open file descriptor for output file
    with open(filename, "wb") as fo:
        # Write to file in chunks
        for chunk in req.iter_content(chunk_size=1024000):
            fo.write(chunk)
    return filename


def _download_tuple(task_tuple):
    """Helper function to download a file, with all params as tuple."""
    return download(task_tuple[0], task_tuple[1])


def get_html(url, session=None):
    """Get the HTML behind an URL.

    Arguments:
    url     -- the URL as a string
    session -- a requests.Session-Object to use, or None if none should be used
    """
    if session is None:
        req = requests.get(url)
    else:
        req = session.get(url)

    if req.status_code != 200:
        print "ERROR: get_html failed, status code", req.status_code, "on URL", url
        return
    return req.text


def get_session():
    """Get a Requests session."""
    return requests.Session()


def pdf_to_text(files):
    """Convert a number of PDFs to text files using pdftotext.

    Arguments:
    files -- a List of files (as paths) to convert
    """
    # Ensure pdftotext is installed
    devnull = open(os.devnull, "w")
    try:
        subprocess.Popen(["pdftotext"], stdout=devnull, stderr=devnull).communicate()
    except OSError as e:
        if e.errno == os.errno.ENOENT:
            print "WARN: Please install pdftotext to enable automatic conversion to text files"
            return
        else:
            raise e

    # pdftotext is installed - Process files
    for file in files:
        if file is None:
            continue
        subprocess.Popen(["pdftotext", "-layout", file], stdout=devnull, stderr=devnull).communicate()
    pass
