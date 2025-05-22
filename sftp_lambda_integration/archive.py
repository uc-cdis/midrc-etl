import datetime
import os
import zipfile
from io import BytesIO


def create_archive(files: list[tuple[str, str]], archive_filename: str) -> None:
    """
    :param files: Tuple of all the files and their in archive paths.
    :param archive_filename: Filepath of resulting archive.
    """
    archive_stream = BytesIO()
    with zipfile.ZipFile(
        archive_stream, "w", compression=zipfile.ZIP_DEFLATED
    ) as zip_archive:
        for filename, in_archive_filename in files:
            last_modified = os.path.getmtime(filename)
            last_modified = datetime.datetime.fromtimestamp(last_modified).timetuple()[
                0:6
            ]
            with open(filename, "rb") as f:
                obj = f.read()
                zip_info = zipfile.ZipInfo(
                    filename=in_archive_filename, date_time=last_modified
                )
                zip_archive.writestr(zip_info, obj)

    with open(archive_filename, "wb") as f:
        f.write(archive_stream.getvalue())
