import os
import urllib2

PART_SIZE = 1024*1024

class HttpFile(object):
    def __init__(self, url):
        self.url = url
        self.size = 0
        self.current_pos = 0
        self.data = {}
        self.populate_token(0)

    def populate_token(self, token):
        req = urllib2.Request(self.url)

        start = token*PART_SIZE
        end = (token+1)*PART_SIZE

        if self.size:
            end = min(end, self.size)

        end -= 1

        req.headers['Range'] = 'bytes=%s-%s' % (start, end)
        r = urllib2.urlopen(req)
        self.size = int(r.info().get('Content-Range').split('/')[1])
        self.data[token] = r.read()

    def token(self, point):
        return point / PART_SIZE

    def read(self, size=2048):
        token = self.token(self.current_pos)
        start = self.current_pos % PART_SIZE
        end = start + size

        if token not in self.data:
            self.populate_token(token)

        data = self.data[token][start:end]

        self.current_pos += size

        return data

    def seek(self, offset, whence=os.SEEK_SET):
        if whence == os.SEEK_SET:
            self.current_pos = offset
        elif whence == os.SEEK_CUR:
            self.current_pos += offset
        elif whence == os.SEEK_END:
            self.current_pos = self.size - offset

    def tell(self):
        return self.current_pos

