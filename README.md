# pdok-mirror

A system for automatically mirroring all documents from the [parliamentary documentation system (pdok)](http://pdok.bundestag.de/) of the german Bundestag. It will download a copy of all documents to the local hard drive (~66 GB of PDFs at the time of writing), create a .txt version of all of them for good measure, and then optionally upload the PDFs to Archive.org (a feature you will most likely not need, as I am already doing that).

## Yes, but... why?
Why not?

Also, given the current trends towards electing populists who would much rather see certain documents scrubbed from the archives, it can never hurt to have a backup of the history of your democracy somewhere safe.

## Legal considerations
In germany, the documents this software is automatically downloading are not covered by Copyright, as they are official state documents ([§ 5 Abs. 2 UrhG](https://www.gesetze-im-internet.de/urhg/__5.html)). However, you are required to:
* distribute them *without modifications* ([§ 62 Abs. 1 bis 3 UrhG](https://www.gesetze-im-internet.de/urhg/__62.html))
* credit the source ([§ 63 Abs. 1 und 2 UrhG](https://www.gesetze-im-internet.de/urhg/__63.html))

## Setup
Install all dependencies (see below) using pip and your distributions package manager (if you want the pdf->text conversion). If you want to use the internetarchive functionality, run ``ia configure`` and enter your internetarchive credentials (but again, you probably don't need that, as I'm already doing that).

## Dependencies
* [peewee](https://github.com/coleifer/peewee) (as the ORM for the local database - licensed under the MIT License)
* [requests](http://docs.python-requests.org/en/master/) (to download the files - licensed under the Apache2)
* [internetarchive](https://internetarchive.readthedocs.io/en/latest/) (to upload to [archive.org](https://archive.org) - licensed under the AGPLv3)
* [python-magic](https://github.com/ahupp/python-magic) (to check the MIME types of downloaded files - licensed under the MIT License)
* ``pdftotext`` installed as a CLI application (for pdf->text conversion - optional, part of ``poppler-utils``, not a python library)

## License
As we use the internetarchive library, which is licensed under the AGPLv3, this software is also licensed AGPLv3. See LICENSE.txt for details.