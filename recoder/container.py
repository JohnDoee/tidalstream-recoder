from StringIO import StringIO

from twisted.internet import defer

class FileContainer(object):
    waiting_for_element = None
    
    def __init__(self):
        self.elements = []
        self.position = 0
        self.size = 0
        self.current_index = 0

    def tell(self):
        return self.position

    @defer.inlineCallbacks
    def seek(self, position):
        self.position = position
        last_size = 0
        last_element = None

        for index, (size, element) in enumerate(self.elements):
            if size > position:
                break

            last_element = element
            last_size = size
        else:
            last_element = element
            last_size = size
            index += 1

        if last_element is None:
            defer.returnValue()
        
        self.current_index = index - 1
        
        self.waiting_for_element = defer.maybeDeferred(last_element.seek, position-last_size)
        yield self.waiting_for_element
        self.waiting_for_element = None

    def write_element(self, element, size):
        self.elements.append((self.size, element))
        self.size += size

    @defer.inlineCallbacks
    def read(self, size=4096):
        if self.waiting_for_element is not None:
            yield self.waiting_for_element
        
        retval = ''
        while size:
            if len(self.elements) <= self.current_index:
                break
            
            element = self.elements[self.current_index][1]
            data = yield defer.maybeDeferred(element.read, size)
            
            if not data:
                break

            if len(data) < size:
                self.current_index += 1
                element.close()
            
            retval += data
            size -= len(data)
            
        self.position += len(retval)

        defer.returnValue(retval)

    def close(self):
        pass
    
    def copy(self):
        fc = FileContainer()
        
        for size, element in self.elements:
            if isinstance(element, StringIO):
                element = StringIO(element.buf)
            else:
                element = element.copy()
            
            fc.elements.append((size, element))
        
        fc.size = self.size
        return fc
