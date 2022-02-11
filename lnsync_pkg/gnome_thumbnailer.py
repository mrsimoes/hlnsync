#!/usr/bin/python3

"""
Force update of the gnome thumbnail for a file and return the icon path.
"""

import os

import gi
gi.require_version('GnomeDesktop', '3.0')
from gi.repository import Gio, GnomeDesktop

class GnomeThumbnailer:
    def __init__(self):
        self.factory = GnomeDesktop.DesktopThumbnailFactory()

    def make_thumbnail(self, filename):
        """
        Return the thumbnail path or raise a RuntimeError exception.
        """
        factory = self.factory
        mtime = os.path.getmtime(filename)
        # Use Gio to determine the URI and mime type
        file_obj = Gio.file_new_for_path(filename)
        uri = file_obj.get_uri()
        info = file_obj.query_info(
            'standard::content-type', Gio.FileQueryInfoFlags.NONE, None)
        mime_type = info.get_content_type()
        thumbnail_path = factory.lookup(uri, mtime)
        if thumbnail_path is not None:
            return thumbnail_path
        if not factory.can_thumbnail(uri, mime_type, mtime):
            raise RuntimeError("cannot make thumbnail for: "+filename)
        thumbnail = factory.generate_thumbnail(uri, mime_type)
        if thumbnail is None:
            raise RuntimeError("error making thumbnail for: "+filename)
        factory.save_thumbnail(thumbnail, uri, mtime)
        thumbnail_path = factory.lookup(uri, mtime)
        assert thumbnail_path is not None
        return thumbnail_path
