import json
import os

from collections import defaultdict
from decimal import Decimal
from functools import partial
from StringIO import StringIO

from ebml.schema.matroska import MatroskaDocument

from twisted.internet import defer, protocol, reactor, task, threads, utils

from .container import FileContainer
from .ebmltools import create_cues_element, create_ebml_header, create_info_element, create_seek_element, create_segment_header_element, create_void, extract_parts
from .httpfile import HttpFile

CUE_OFFSET = 50000 # padding to the cue table to make sure there is room (better safe than sorry)
OUTPUT_FORMAT = 'output-%05d.mkv'

class FFMpegPP(protocol.ProcessProtocol):
    def __init__(self):
        self.finished = defer.Deferred()
    
    def connectionMade(self):
        print "connectionMade!"

    def outReceived(self, data):
        pass
        #print "outReceived! with %d bytes!" % len(data)
        #print data

    def errReceived(self, data):
        pass
        #print "errReceived! with %d bytes!" % len(data)
        #print data

    def inConnectionLost(self):
        print "inConnectionLost! stdin is closed! (we probably did it)"

    def outConnectionLost(self):
        print "outConnectionLost! The child closed their stdout!"

    def errConnectionLost(self):
        print "errConnectionLost! The child closed their stderr."

    def processExited(self, reason):
        self.processEnded(reason)
        print "processExited, status %d" % (reason.value.exitCode,)

    def processEnded(self, reason):
        if not self.finished.called:
            if reason.value.exitCode == 0:
                self.finished.callback(None)
            else:
                self.finished.errback(None)
        
        print "processEnded, status %d" % (reason.value.exitCode,)

def wrap_segment(filepath, expected_size):
    with open(filepath, 'rb') as f:
        doc = MatroskaDocument(f)
        s = doc.roots[1]
        retval = ''
        for element in s.value:
            if element.name == 'Cluster':
                element.stream.seek(0)
                retval += element.stream.read(element.size)
        retval += create_void(expected_size - len(retval))
    
    return StringIO(retval)

class LazyStringIO(object):
    data = None
    data_setting_defer = None

    def __init__(self, data_fetch_function, size):
        self.data_fetch_function = data_fetch_function
        self.size = size

    @defer.inlineCallbacks
    def set_data(self):
        if self.data_setting_defer is not None:
            yield self.data_setting_defer
        else:
            self.data_setting_defer = defer.Deferred()
            self.data = yield defer.maybeDeferred(self.data_fetch_function)
            self.data_setting_defer.callback(None)
            self.data_setting_defer = None

    @defer.inlineCallbacks
    def read(self, size=1024):
        if self.data is None:
            yield self.set_data()
        defer.returnValue(self.data.read(size))

    @defer.inlineCallbacks
    def seek(self, offset, whence=os.SEEK_SET):
        if self.data is None:
            yield self.set_data()
        
        self.data.seek(offset, whence)

    def tell(self):
        if self.data is None:
            self.set_data()
        
        self.data.tell()
    
    def close(self):
        self.data = None
    
    def copy(self):
        return LazyStringIO(self.data_fetch_function, self.size)

class Encoder(object): # must always seperate subs / no room for custom fonts etc.
    format = None
    segment_count = None # number of segments, last id will be <this>-1
    file_info = None
    
    move_timer = None
    ffmpeg_process = None
    
    base_container = None
    
    def __init__(self, url, output_path, ffmpeg_path, ffprobe_path):
        self.url = url
        self.ffmpeg_path = ffmpeg_path
        self.ffprobe_path = ffprobe_path
        self.output_path = output_path
        self.temp_output_path = os.path.join(output_path, 'encoding')
        
        self.move_timer = task.LoopingCall(self.check_for_files_to_move)
        
        self.container_defers = []
        self.segment_created_defers = dict()
        
        if not os.path.isdir(self.temp_output_path):
            os.mkdir(self.temp_output_path)
        
        self.streams = defaultdict(list)
    
    def _get_ffprobe_output(self):
        return utils.getProcessOutput(self.ffprobe_path, args=[
            '-print_format', 'json',
            '-loglevel', 'quiet',
            '-show_format',
            '-show_streams',
            self.url,
        ])
    
    @defer.inlineCallbacks
    def probe(self):
        output = yield self._get_ffprobe_output()
        output = json.loads(output)
        
        for track in output['streams']:
            self.streams[track['codec_type']].append(track)
        
        self.format = output['format']
    
    def check_if_ready_to_stream(self):
        if self.file_info is None or 'Tracks' not in self.file_info:
            return
        
        self.build_container()
    
    def estimate_if_should_encode_from_elsewhere(self, segment_id):
        """
        Sometimes we will need to kill the re-encode and start elsewhere, e.g. if a client seeks.
        
        This function will try to guess if we should wait or restart
        """
        pass
    
    def get_segment(self, segment_id, expected_size):
        filepath = os.path.join(self.output_path, OUTPUT_FORMAT % segment_id)
        if os.path.isfile(filepath):
            return defer.succeed(wrap_segment(filepath, expected_size))
        else: # check if we need to cancel our encode and start elsewhere
            if segment_id not in self.segment_created_defers:
                self.segment_created_defers[segment_id] = []
            
            d = defer.Deferred()
            self.segment_created_defers[segment_id].append(d)
            
            def make_wrap_segment(ignored):
                return defer.succeed(wrap_segment(filepath, expected_size))
            d.addCallback(make_wrap_segment)
            
            return d
    
    def get_container(self):
        if self.base_container:
            return defer.succeed(self.base_container.copy())
        else:
            d = defer.Deferred()
            self.container_defers.append(d)
            return d
    
    def _create_segment_header(self):
        timecodescale, duration = None, None
        
        for element in self.file_info['Info']:
            if element.name == 'TimecodeScale':
                timecodescale = element.value
            
            elif element.name == 'Duration':
                duration = element.value
        
        tracks_element = self.file_info['Tracks']
        info_element = create_info_element(timecodescale, duration)
        seek_element = create_seek_element(info_element, tracks_element)
        cues_element = create_cues_element(self.file_info['Cues'], CUE_OFFSET)
        
        retval = seek_element
        retval += info_element
        retval += create_void(100+len(info_element)-len(retval))
        retval += tracks_element
        retval += cues_element
        start_of_cue = sorted(self.file_info['Cues'].items())[0][1] + CUE_OFFSET
        retval += create_void(start_of_cue-len(retval))
        
        segment_size = self.file_info['Size'] + CUE_OFFSET * 2
        segment_val = create_segment_header_element(segment_size)
        segment_header_size = len(segment_val)
        segment_val += retval
        
        return segment_val, segment_size, segment_header_size
    
    def build_container(self):
        if self.base_container:
            return
        
        ebml_header_element = create_ebml_header()
        segment_header_element, segment_size, segment_header_size = self._create_segment_header()
        
        cluster_start = ebml_header_element + segment_header_element
        
        self.filesize = len(ebml_header_element) + segment_size + segment_header_size
        container = FileContainer()
        container.write_element(StringIO(cluster_start), len(cluster_start))
        
        last_size = None
        i = 0
        for v in sorted(self.file_info['Cues'].values()):
            v += CUE_OFFSET
            if last_size is not None:
                size = v-last_size
                container.write_element(LazyStringIO(partial(self.get_segment, i, size), size), size)
                i += 1
            last_size = v
        
        size = segment_size-last_size
        container.write_element(LazyStringIO(partial(self.get_segment, i, size), size), size)
        
        self.base_container = container
        for d in self.container_defers:
            d.callback(self.base_container.copy())
    
    @defer.inlineCallbacks
    def probe_tracks(self, filepath):
        if self.file_info is not None and 'Tracks' in self.file_info:
            return
        
        with open(filepath, 'rb') as f:
            doc = MatroskaDocument(f)
            segment = doc.roots[1]
            
            info = yield threads.deferToThread(extract_parts, segment, parts=['Tracks'])
            
            self.file_info['Tracks'] = info['Tracks']
        
        self.check_if_ready_to_stream()
    
    def _get_segment_id_from_filename(self, filename):
        filename = filename.split('.')[0]
        
        try:
            segment_id = int(filename.split('-')[1])
        except ValueError:
            return None
        
        return segment_id
    
    def check_for_files_to_move(self, move_last=True):
        files = sorted(os.listdir(self.temp_output_path))
        
        for i, filename in enumerate(files, 1):
            segment_id = self._get_segment_id_from_filename(filename)
            filepath = os.path.join(self.temp_output_path, filename)
            
            if segment_id is None: # invalid filename, got no segment id, skipping
                continue 
            
            if segment_id < self.start_segment_id: # this is a useless file, it needs to be deleted if it is not in use
                if len(files) > 1:
                    os.remove(filepath)
                continue
            
            if i == len(files): # this is the last file, we cannot move that
                if not move_last or segment_id != self.end_segment_id: # enables the ability to move the last file
                    continue
            
            result_file = os.path.join(self.output_path, filename)
            os.rename(filepath, result_file)
            
            self.probe_tracks(result_file)
            
            if segment_id in self.segment_created_defers:
                for d in self.segment_created_defers[segment_id]:
                    d.callback(None)
                
                del self.segment_created_defers[segment_id]
    
    def clean_temp_output_folder(self):
        for f in os.path.listdir(self.temp_output_path): # make sure the output folder is empty
            os.remove(os.path.join(self.temp_output_path, f))
    
    def start_encoding(self, start_segment_id, end_segment_id=None): # 'output-%05d.mkv'
        self.start_segment_id = start_segment_id
        self.end_segment_id = end_segment_id
        
        if self.end_segment_id is None:
            self.end_segment_id = len(self.cue_times)-1
        
        self.move_timer.start(1.0)
        
        cmd = [
            self.ffmpeg_path, '-i', self.url, '-sn', '-codec', 'copy', '-map', '0',
            '-c:a', 'aac', '-strict', '-2', '-b:a', '384k',
            '-f', 'segment', '-segment_format', 'mkv',
            '-segment_times', ','.join(self.cue_times[start_segment_id+1:]),
        ]
        
        if start_segment_id > 0:
            initial_offset = self.cue_times[start_segment_id]
            
            cmd += [
                '-segment_start_number', str(start_segment_id-1),
                '-initial_offset', str(initial_offset),
                '-ss', str(initial_offset),
            ]
        
        if end_segment_id is not None:
            cmd += [
                '-to', self.cue_times[end_segment_id+1],
            ]
        
        cmd += [
            os.path.join(self.temp_output_path, OUTPUT_FORMAT),
        ]
        
        self.ffmpeg_process = FFMpegPP()
        
        reactor.spawnProcess(self.ffmpeg_process, self.ffmpeg_path, cmd)
        
        def done_encoding(ignored):
            self.stop_encoding(successful=True)
        
        self.ffmpeg_process.finished.addCallback(done_encoding)
    
    def stop_encoding(self, successful=False):
        self.move_timer.stop()
        
        if not successful:
            self.ffmpeg_process.transport.signalProcess('KILL')
            pass # kill process
        
        self.check_for_files_to_move(move_last=successful)
        
        # kills the encoding, needed if we need to start from new segment
        # stop move timer, run check_for_files_to_move, cleanup temp folder
        pass
    
    @defer.inlineCallbacks
    def extract_info(self):
        httpfile = HttpFile(self.url)
        doc = MatroskaDocument(httpfile)
        segment = doc.roots[1]
        self.file_info = yield threads.deferToThread(extract_parts, segment, parts=['Cues', 'Info'])
        self.file_info['Size'] = segment.size
        
        self.cue_times = [str(Decimal(k)/1000) for k in sorted(self.file_info['Cues'].keys())]
    
    @defer.inlineCallbacks
    def prepare_encode(self):
        yield self.extract_info()
        self.start_encoding(0) # need to continue when first element is moved
    # encode

if __name__ == '__main__':
    import sys
    
    encoder = Encoder(sys.argv[1], 'unpack/tmp/', './ffmpeg', './ffprobe')
    encoder.prepare_encode()
    
    @defer.inlineCallbacks
    def got_container(container):
        with open('testoutput.mkv', 'wb') as f:
            while True:
                data = yield container.read()
                if not data:
                    break
                
                f.write(data)
    encoder.get_container().addCallback(got_container)
    
    reactor.run()