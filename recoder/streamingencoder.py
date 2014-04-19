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
from .encoder import FFMpegPP, wrap_segment, LazyStringIO, Encoder
from .httpfile import HttpFile

OUTPUT_FORMAT = 'output-%05d.mkv'

class StreamingEncoder(Encoder):
    def _create_segment_header(self):
        timecodescale, duration = None, None
        
        for element in self.file_info['Info']:
            if element.name == 'TimecodeScale':
                timecodescale = element.value
            
        duration = Decimal(self.format['duration']) * 1000000000 / timecodescale
        
        tracks_element = self.file_info['Tracks']
        info_element = create_info_element(timecodescale, duration)
        
        retval = create_segment_header_element(None)
        retval += info_element
        retval += tracks_element
        
        return retval
    
    def _get_segment_size(self, segment_id):
        filepath = os.path.join(self.output_path, OUTPUT_FORMAT % segment_id)
        return os.path.getsize(filepath)
    
    def build_container(self):
        if self.base_container:
            return
        
        ebml_header_element = create_ebml_header()
        segment_header_element = self._create_segment_header()
        cluster_start = ebml_header_element + segment_header_element
        
        container = FileContainer()
        container.write_element(StringIO(cluster_start), len(cluster_start))
        
        for filename in os.listdir(self.output_path):
            if not filename.startswith('output'):
                continue
            
            segment_id = self._get_segment_id_from_filename(filename)
            size = self._get_segment_size(segment_id)
            container.write_element(LazyStringIO(partial(self.get_segment, segment_id, size), size), size)
        
        self.base_container = container
        for d in self.container_defers:
            d.callback(self.base_container.copy())
    
    def check_for_files_to_move(self, move_last=True):
        files = sorted(os.listdir(self.temp_output_path))
        
        for i, filename in enumerate(files, 1):
            segment_id = self._get_segment_id_from_filename(filename)
            filepath = os.path.join(self.temp_output_path, filename)
            
            if segment_id is None: # invalid filename, got no segment id, skipping
                continue 
            
            if i == len(files) and self.ffmpeg_process is not None: # this is the last file, we cannot move that
                continue
            
            result_file = os.path.join(self.output_path, filename)
            os.rename(filepath, result_file)
            
            self.probe_tracks(result_file)
            
            if segment_id in self.segment_created_defers:
                for d in self.segment_created_defers[segment_id]:
                    d.callback(None)
                
                del self.segment_created_defers[segment_id]
            
            if self.base_container:
                size = self._get_segment_size(segment_id)
                self.base_container.write_element(LazyStringIO(partial(self.get_segment, segment_id, size), size), size)
    
    def start_encoding(self): # 'output-%05d.mkv'
        self.move_timer.start(1.0)
        
        cmd = [
            self.ffmpeg_path, '-i', self.url, '-sn', '-codec', 'copy', '-map', '0',
            '-c:a', 'aac', '-strict', '-2', '-b:a', '384k',
            '-f', 'segment', '-segment_format', 'mkv',
            '-segment_time', '10',
        ]
        
        cmd += [
            os.path.join(self.temp_output_path, OUTPUT_FORMAT),
        ]
        
        self.ffmpeg_process = FFMpegPP()
        
        reactor.spawnProcess(self.ffmpeg_process, self.ffmpeg_path, cmd)
        
        def done_encoding(ignored):
            self.ffmpeg_process = None
            self.stop_encoding(successful=True)
        
        self.ffmpeg_process.finished.addCallback(done_encoding)
    
    @defer.inlineCallbacks
    def probe_tracks(self, filepath):
        if self.file_info is not None and 'Tracks' in self.file_info:
            return
        
        with open(filepath, 'rb') as f:
            doc = MatroskaDocument(f)
            segment = doc.roots[1]
            
            self.file_info = yield threads.deferToThread(extract_parts, segment, parts=['Tracks', 'Info'])
        
        self.check_if_ready_to_stream()
    
    @defer.inlineCallbacks
    def prepare_encode(self):
        yield self.probe()
        self.start_encoding()
    
    def stop_encoding(self, successful=False):
        self.move_timer.stop()
        
        if not successful:
            self.ffmpeg_process.transport.signalProcess('KILL')
        
        self.check_for_files_to_move(move_last=successful)
        
        # Generate cue table here and add it to the container.