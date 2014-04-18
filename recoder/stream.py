import os
import uuid

from twisted.web import resource, server

from .encoder import Encoder
from .streamingencoder import StreamingEncoder
from .encoder import Encoder
from .filelike import FilelikeObjectResource

class Stream(resource.Resource):
    isLeaf = True
    
    def __init__(self, url, streaming_encode, output_folder, ffmpeg_path, ffprobe_path):
        self.identifier = str(uuid.uuid4())
        self.url = url
        
        output_folder = os.path.join(output_folder, self.identifier)
        os.mkdir(output_folder)
       
        if streaming_encode:
            cls = StreamingEncoder
        else:
            cls = Encoder
        
        self.encoder = cls(url, output_folder, ffmpeg_path, ffprobe_path)
        self.encoder.prepare_encode()
        
        resource.Resource.__init__(self)
    
    def render_GET(self, request):
        def got_container(container):
            FilelikeObjectResource(container, container.get_size()).render(request)
        
        self.encoder.get_container().addCallback(got_container)
        return server.NOT_DONE_YET