# -*- encoding: utf-8 -*-
"""Models for the database of the pdok-crawler.

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

from peewee import *


db = SqliteDatabase('pdoc.sqlite', threadlocals=True)


class Wahlperiode(Model):
    """Model for keeping track of different election periods."""

    # Identifier in database
    dbid = PrimaryKeyField()

    # Wahlperiodennummer
    period_no = CharField(index=True)

    # Highest seen Plenarprotokoll number
    plenary_max = IntegerField(default=0)
    # Highest seen Drucksache number
    drucksache_max = IntegerField(default=0)

    # Period scraped
    period_scraped = BooleanField(default=False)
    # Period uploaded to Archive
    period_uploaded = BooleanField(default=False)

    class Meta:
        """Meta information about model."""

        database = db


class Document(Model):
    """Base model for more specific document types."""

    # Database identifier
    dbid = PrimaryKeyField()

    # Document number (e.g. 18/001 or 17/14600)
    docno = CharField(index=True)
    # Internet Archive Identifier
    archive_ident = CharField(null=True)
    # Title of the document
    title = CharField()
    # Veröffentlichungsdatum
    date = DateTimeField()
    # Path to the file
    path = CharField()
    # Source URL
    source = CharField()

    class Meta:
        """Meta information about model."""

        database = db


class Drucksache(Document):
    """Drucksache - Anfrage, Gesetz, ..."""

    # Doctype - Kleine Anfrage, Gesetz, ...
    doctype = CharField()
    # Urheber - originating legal body
    urheber = CharField(null=True)
    # Autor - actual person(s) who wrote this
    autor = CharField(null=True)
    # Wahlperiode (currently 01,02,...,18)
    period = ForeignKeyField(Wahlperiode, related_name='drucksachen')


class Plenarprotokoll(Document):
    """Plenarprotokoll."""

    # Wahlperiode (currently 01,02,...,18)
    period = ForeignKeyField(Wahlperiode, related_name='plenarprotokolle')
    pass


def setup():
    """Set up the database connection."""
    db.connect()
    db.create_tables([Wahlperiode, Document, Drucksache, Plenarprotokoll], safe=True)

setup()
