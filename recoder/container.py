from StringIO import StringIO

from twisted.internet import defer

class FileContainer(object):
    waiting_for_element = None
    parent = None
    
    def __init__(self):
        self.elements = []
        self.position = 0
        self.size = 0
        self.current_index = 0
        self.done = False
        self.new_element_added = defer.Deferred()
        self.children = []

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
        self.new_element_added.callback(None)
        self.new_element_added = defer.Deferred()
        
        for child in self.children:
            child.write_element(self._get_element_copy(element), size)

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
        
        if retval == '' and size and not self.done: # there will be new elements added soon
            yield self.new_element_added
            if not self.done:
                retval += yield self.read(size)

        defer.returnValue(retval)

    def get_size(self):
        if self.done:
            return self.size
        else:
            return 0
    
    def close(self):
        if self.parent and self in self.parent.children:
            self.parent.children.remove(self)
    
    def _get_element_copy(self, element):
        if isinstance(element, StringIO):
            element = StringIO(element.buf)
        else:
            element = element.copy()
        return element
    
    def copy(self):
        fc = FileContainer()
        self.children.append(fc)
        
        for size, element in self.elements:
            element = self._get_element_copy(element)
            fc.elements.append((size, element))
        
        fc.size = self.size
        return fc
