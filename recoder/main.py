import urllib

from twisted.web import resource, server, http, error, util
from twisted.internet import reactor

from .stream import Stream

class MainResource(resource.Resource):
    isLeaf = False
    streams = {}
    urlmap = {}

    def __init__(self, output_folder, ffmpeg_path, ffprobe_path):
        self.output_folder = output_folder
        self.ffmpeg_path = ffmpeg_path
        self.ffprobe_path = ffprobe_path
        
        resource.Resource.__init__(self)
    
    def getChild(self, path, request):
        path = path.strip('/')
        if path in self.streams:
            return self.streams[path]
        elif not path:
            return self
        else:
            return resource.NoResource()

    def render_GET(self, request):
        if 'url' not in request.args:
            raise error.Error(http.BAD_REQUEST, 'Missing argument: url')
        url = request.args['url'][0]

        if url not in self.urlmap:
            stream = Stream(url, self.output_folder, self.ffmpeg_path, self.ffprobe_path)
            identifier = stream.identifier
            self.streams[identifier] = stream
            self.urlmap[url] = identifier
        
        filename = url.split('?')[0].split('/')[-1]
        return util.redirectTo('/%s/%s' % (self.urlmap[url], urllib.quote(filename)), request)

if __name__ == '__main__':
    res = MainResource()
    factory = server.Site(res)
    reactor.listenTCP(8888, factory)
    reactor.run()
