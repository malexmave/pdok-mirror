# -*- encoding: utf-8 -*-
"""Upload files to InternetArchive.

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

from internetarchive import upload
from models.database import Wahlperiode, Drucksache, Plenarprotokoll
from multiprocessing.pool import ThreadPool


def upload_legislaturperiode(period_no_numeric):
    """Upload all files from a legislaturperiode.

    Files that have already been uploaded are ignored.
    """
    # Format period number properly
    period_no = '%02d' % period_no_numeric
    # Get from database or create
    period = Wahlperiode.get(period_no=period_no)

    # If the period has already been scraped, return instantly
    if period.period_uploaded:
        print "INFO: Period", period_no, "has already been uploaded - skipping"
        return

    # Upload all plenary protocols
    upload_plenarprotokoll(period)


def upload_plenarprotokoll(period):
    """Upload all plenary protocols belonging to the given period."""
    to_upload = []
    for plenary in Plenarprotokoll.select().where(Plenarprotokoll.period == period):
        if plenary.archive_ident is not None:
            continue
        metadata = dict(collection='deutscherbundestag',
                        title=plenary.docno + " - " + plenary.title,
                        mediatype='texts',
                        source=u'<a href="http://pdok.bundestag.de/" target="blank">Parlamentarisches Dokumentationssystem</a>',
                        contributor=u'<a href="https://twitter.com/malexmave">@malexmave</a>',
                        rights=u'Free to republish without modification as long as the source is credited, as per § 5 Abs. 2 UrhG',
                        publisher=u"Deutscher Bundestag",
                        creator=u'Deutscher Bundestag',
                        credits=u"Steganografischer Dienst des Bundestages",  # TODO Add authors / steganografischer Dienst here
                        description=u'<p>Plenarprotokoll des deutschen Bundestages vom ' + str(plenary.date) + u'.</p><br><p>Automatically mirrored from the german <a href="http://pdok.bundestag.de/" target="blank">parliamentary documentation system</a>. Reproduction without modification allowed as long as the source is credited (according to § 5 Abs. 2 of the german Urheberrecht).</p><p>This is not the authoritative version, but an unofficial mirror. Please check the primary sources when in doubt.</p><p>This post was automatically created using <a href="https://github.com/malexmave/pdok-mirror" target="blank">pdok-mirror</a> and the python <a href="https://internetarchive.readthedocs.io/en/latest/" target="blank">internetarchive</a> library.</p>',  # TODO Update
                        language=u"ger",
                        subject=["Deutscher Bundestag", "Plenarprotokoll", "Legislaturperiode " + str(period.period_no)]
                        )
        identifier = 'ger-bt-plenary-' + plenary.docno.replace('/', '-')
        to_upload += [(identifier, {plenary.path.split('/')[-1]: plenary.path}, metadata, plenary)]
    pool = ThreadPool(processes=10)
    results = pool.map(upload_parallel, to_upload)
    for result in results:
        if result is None:
            continue
        plenary, identifier = result
        plenary.archive_ident = identifier
        print "DEBUG: Successfully uploaded", identifier
        plenary.save()


def upload_parallel(params):
    """Helper function to upload stuff in parallel."""
    if params is None or len(params) != 4:
        print "ERROR: Bad parameters"
        return
    identifier, files, metadata, plenary = params
    r = upload(identifier, files=files, metadata=metadata, retries=5)
    if r[0].status_code != 200:
        print "ERROR: Upload of", identifier, "failed:", r[0].status_code
        return None
    else:
        print "DEBUG: Uploaded", identifier
        return (plenary, identifier)


# metadata = dict(collection='test_collection',  # TODO Update
#                 title='Test upload 13/37 - Hurr durr herp derp doodle',  # TODO Update
#                 mediatype='texts',
#                 source='<a href="http://pdok.bundestag.de/" target="blank">Parlamentarisches Dokumentationssystem</a>',
#                 contributor='<a href="https://twitter.com/malexmave">@malexmave</a>',
#                 rights='Free to republish without modification as long as the source is credited, as per § 5 Abs. 2 UrhG',
#                 publisher="Deutscher Bundestag",
#                 creator='Deutscher Bundestag',
#                 credits="",  # TODO Add authors / steganografischer Dienst here
#                 description='<p>Automatically mirrored from the german <a href="http://pdok.bundestag.de/" target="blank">parliamentary documentation system</a>. Reproduction without modification allowed as long as the source is credited (according to § 5 Abs. 2 of the german Urheberrecht).</p><p>This is not the authoritative version, but an unofficial mirror. Please check the primary sources when in doubt.</p><p>This post was created using <a href="https://github.com/malexmave/pdok-mirror" target="blank">pdok-mirror</a> and the python <a href="https://internetarchive.readthedocs.io/en/latest/" target="blank">internetarchive</a> library.</p>',  # TODO Update
#                 language="ger",
#                 subject=["Deutscher Bundestag", "Drucksache"]  # TODO Add Parlamentsprotokoll, Kleine Anfrage, ...
#                 )
# r = upload('malexmave-test-upload-ignore3', files={'testfile.txt': 'testfile.txt'}, metadata=metadata, retries=5)  # TODO Update
# print r[0].status_code
