from datetime import datetime

from ebml.core import *
from ebml.schema.matroska import _Elements
from ebml.schema.base import INT, UINT, FLOAT, STRING, UNICODE, DATE, BINARY, CONTAINER

def encode_date(date, length=None):
    if date is None:
        date = datetime.utcnow()
    elif date.utcoffset():
        date = (date - date.utcoffset()).replace(tzinfo=None)
    if length is None:
        length = 8
    elif length != 8:
        raise ValueError('Cannot encode date value %s with any length other than 8 bytes.')
    
    delta = date - datetime(2001, 1, 1, tzinfo=None)
    nanoseconds = (delta.microseconds + ((delta.seconds + (delta.days * 24 * 60 * 60)) * 10**6)) * 10**3
    return encode_signed_integer(nanoseconds, length)

def read_elements_iter(stream, document, children, offset=0):
    size = stream.size - offset
    while size:
        element_offset = stream.size - size
        stream.seek(element_offset)
        element_id, element_id_size = read_element_id(stream)
        element_size, element_size_size = read_element_size(stream)
        element_stream_size = element_id_size + element_size_size + element_size
        element_stream = stream.substream(element_offset, element_stream_size)
        size -= element_stream_size
    
        element_class = None
        for child in (children + document.globals):
            if child.id == element_id:
                element_class = child
                break
    
        if element_class is None:
            element = UnknownElement(document, element_stream, element_id)
        else:
            element = element_class(document, element_stream)
        yield element

def get_segment_iter(segmentelement, offset=0):
    if segmentelement.type != CONTAINER:
        raise Exception('Not a container')

    return read_elements_iter(segmentelement.body_stream, segmentelement.document,
                              segmentelement.children, offset)

WRITERS = {
        INT: encode_signed_integer,
        UINT: encode_unsigned_integer,
        FLOAT: encode_float,
        STRING: encode_string,
        UNICODE: encode_unicode_string,
        DATE: encode_date,
        BINARY: lambda stream: bytearray(stream)
}

class NoUsefulInfoFoundException(Exception):
    pass

def get_element(name):
    return _Elements['%sElement' % name]

def encode_elements(elements):
    retval = ''
    for element in elements:
        if element.type == CONTAINER:
            data = encode_elements(element.value)
        else:
            writer = WRITERS[element.type]
            data = writer(element.value)

        retval += encode_element_id(element.id)
        retval += encode_element_size(len(data))
        retval += data

    return retval

def encode_container(element):
    retval = ''
    for k, v in element:
        e = get_element(k)
        if e.type == CONTAINER:
            data = encode_container(v)
        else:
            writer = WRITERS[e.type]
            data = writer(v)

        retval += encode_element_id(e.id)
        retval += encode_element_size(len(data))
        retval += data
    return retval

def create_segment_header_element(size):
    return encode_element_id(get_element('Segment').id) + encode_element_size(size)

def create_info_element(timecodescale, duration):
    return encode_container([('Info', [
        ('TimecodeScale', timecodescale),
        ('MuxingApp', 'The Tidal Streamer'),
        ('WritingApp', 'The Tidal Streamer'),
        ('Duration', duration),
        ('DateUTC', datetime.now()),
        ('SegmentUID', '1234567890123456'),
    ])])

def create_seek_element(info, tracks):
    positions = [
        ('Tracks', 100+len(info)),
        ('Cues', 100+len(info)+len(tracks)),
    ]
    
    search_segments = []
    for element, position in positions:
        e = get_element(element)
        search_segments.append(('SeekPoint', [('SeekID', encode_element_id(e.id)), ('SeekPosition', position)]))
    
    return encode_container([('SeekHeader', search_segments)])

def create_cues_element(cues, size_offset=0):
    elements = []
    for k, v in sorted(cues.items()):
        elements.append(('CuePoint', [
                            ('CueTime', k),
                            ('CueTrackPositions', [('CueTrack', 1), ('CueClusterPosition', v+size_offset)])
                         ]))
    return encode_container([('Cues', elements)])

def create_ebml_header():
    return encode_container([
        ('EBML', [
            ('EBMLVersion', 1),
            ('EBMLReadVersion', 1),
            ('EBMLMaxIDLength', 4),
            ('EBMLMaxSizeLength', 8),
            ('DocType', 'matroska'),
            ('DocTypeVersion', 2),
            ('DocTypeReadVersion', 2)])])

def create_void(size):
    if size == 129:
        return create_void(100) + create_void(29)

    if size == 16131:
        return create_void(10000) + create_void(6131)

    offset = 2
    if size >= 130:
        offset = 3
    if size >= 16132:
        offset = 4
    if size >= 2031621:
        offset = 5
    
    return encode_container([('Void', bytearray(size-offset))])

def extract_cuedata(segmentelement, offset):
    positions = {}
    elements = get_segment_iter(segmentelement, offset)
    for element in elements:
        if element.name != 'Cues':
            break

        for cuepos in element.value:
            element_values = {'CueTime': None, 'CueTrackPositions': None}
            for value in cuepos.value:
                element_values[value.name] = value.value
            element_values['CueTrackPositions'] = [v.value for v in element_values['CueTrackPositions'] if v.name == 'CueClusterPosition'] or None
            if element_values['CueTime'] is not None and element_values['CueTrackPositions'] is not None:
                positions[element_values['CueTime']] = element_values['CueTrackPositions'][0]

    return positions

def extract_parts(segmentelement, parts):
    seiter = get_segment_iter(segmentelement)
    seekhead = next(seiter)
    retval = {}

    if seekhead.name != 'SeekHead':
        raise NoUsefulInfoFoundException('First element name is not SeekHead, it is %r' % seekhead.name)

    for element in seekhead.value:
        element_id, seekposition = [x.value for x in element.value]
        if element_id == encode_element_id(get_element('Segment').id) and 'Segment' in parts: # '\x15\x49\xa9\x66'
            retval['Segment'] = get_segment_iter(segmentelement, seekposition)
        elif element_id == encode_element_id(get_element('Tracks').id) and 'Tracks' in parts: # '\x16\x54\xae\x6b'
            retval['Tracks'] = encode_elements([next(get_segment_iter(segmentelement, seekposition))])
        elif element_id == encode_element_id(get_element('Cues').id) and 'Cues' in parts: # '\x1c\x53\xbb\x6b'
            retval['Cues'] = extract_cuedata(segmentelement, seekposition)
        #else:
        #    print 'Unused element %r/%r' % (binascii.hexlify(element_id), seekposition)

    if 'Info' in parts:
        for element in seiter:
            if element.name == 'Cluster':
                break
            elif element.name ==  'Info':
                retval['Info'] = element.value

    return retval