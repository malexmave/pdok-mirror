# -*- encoding: utf-8 -*-
"""Utility functions for downloading files, ...

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

import requests
import os.path
import subprocess
import time
import magic


def download(url, filename, session=None, retry=0):
    """Download a file, if it exists.

    Arguments:
    url      -- the url to download
    filename -- the filename to which the file should be saved
    session  -- a requests.Session-Object to use (to persist connections)
    retry    -- Retry count
    """
    # Check if file already exists
    if os.path.isfile(filename):
        # Check if file is indeed a PDF file
        # File already exists, just return a reference to it.
        # (already processed files will be ignored by processing)
        if is_pdf(filename):
            return filename
        # If this statement is reached, the file exists but isn't a .pdf
        # Delete the file and any converted plaintext version, if it exists
        os.remove(filename)
        if os.path.isfile(filename[:-4] + ".txt"):
            os.remove(filename[:-4] + ".txt")

    # Start downloading the file in streaming mode, to save memory
    # Do this in an endless loop to catch any connection errors and retry
    while True:
        try:
            if session is None:
                # No session passed, create new connection
                req = requests.get(url, stream=True)
            else:
                # Reuse old session
                req = session.get(url, stream=True)
            break
        except requests.exceptions.ConnectionError:
            # We got a connection error. Sleep 1 second and try again.
            time.sleep(1)

    # Check if the file actually exists
    if req.status_code == 404:
        return None

    # Open file descriptor for output file
    with open(filename, "wb") as fo:
        # Write to file in chunks
        for chunk in req.iter_content(chunk_size=1024000):
            fo.write(chunk)

    # Check if we have actually downloaded a PDF file
    if not is_pdf(filename):
        if retry >= 3:
            print "ERROR: Downloaded file", filename, "appears to not be a PDF. Using anyway."
            return filename
        time.sleep(1)
        download(url, filename, session, retry + 1)
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
    while True:
        try:
            if session is None:
                req = requests.get(url)
            else:
                req = session.get(url)
            break
        except requests.exceptions.ConnectionError:
            time.sleep(1)

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
        if os.path.isfile(file[:-4] + ".txt"):
            continue
        subprocess.Popen(["pdftotext", "-layout", file], stdout=devnull, stderr=devnull).communicate()
    pass


def is_pdf(filepath):
    """Check if a file is a PDF file.

    Arguments
    filepath -- path to the file to check
    """
    if filepath is None or not os.path.isfile(filepath):
        return False

    mime = magic.Magic(mime=True)
    return mime.from_file(filepath) == "application/pdf"
