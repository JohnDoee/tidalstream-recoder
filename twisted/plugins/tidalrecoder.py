import os

from twisted.application import internet
from twisted.application.service import IServiceMaker
from twisted.plugin import IPlugin
from twisted.python import usage
from twisted.web import server

from zope.interface import implements


class Options(usage.Options):
    optParameters = [
        ['ffprobe', 'fp', './ffprobe', "Path to ffprobe"],
        ['ffmpeg', 'fm', './ffmpeg', "Path to ffmpeg"],
        ['folder', 'f', './unpack', "Path to store encoded"],
        ['port', 'p', '8888', "Port to listen on"],
    ]

class TidalRecoderServiceMaker(object):
    implements(IServiceMaker, IPlugin)
    tapname = "tidalrecoder"
    description = "Codename TidalStream Re-Encoder Proxy."
    options = Options

    def makeService(self, options):
        from recoder.main import MainResource
        
        site = server.Site(MainResource(options['folder'], options['ffmpeg'], options['ffprobe']))
        
        return internet.TCPServer(int(options['port']), site)

serviceMaker = TidalRecoderServiceMaker()
