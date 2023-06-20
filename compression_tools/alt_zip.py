# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 16:02:10 2023

@author: awatson
"""

import dask
from dask import delayed
import hashlib
import json
import time
import glob
import os
from zipfile import ZipFile
from numcodecs import Blosc
from io import BytesIO
from pprint import pprint as print

import argparse
import psutil
psutil.virtual_memory()

# parser = argparse.ArgumentParser(description='''
#                                  Recursively combine a directory into a ZIP
#                                  file using configurable compression
#                                  ''')

# positional = [
#     ('input_dir',str,'+','One input directorory.'),
#     ]

# optional = [
    
#     (['-out','--output_zip'],str,1,'OUT',None,'store','Output ZIP file name'),
#     (['-cpu'],int,1,'C',os.cpu_count(),'store','Number of cpus which are available'),

#     # Compression options
#     (['-cmp','--compression'],str,1,'CMP','zstd','store','Compression method from Blosc library (zstd (default),blosclz, lz4, lz4hc, zlib or snappy)'),
#     (['-cl','--clevel'],int,1,'CLV',5,'store','Compression level : Integer 0-9 (default 5)'),
#     (['-sh','--shuffle'],int,1,'SHF',1,'store','Shuffle option integer: NOSHUFFLE (0), SHUFFLE (1), BITSHUFFLE (2) or AUTOSHUFFLE (-1) (default 1)'),
#     (['-bk','--blocksize'],int,1,'BLK',0,'store','The requested size of the compressed blocks. If 0 (default), an automatic blocksize will be used'),
#     ]

# switch = [
#     (['-v', '--verbose'], 0,'count','Verbose output : additive more v = greater level of verbosity'),
#     (['-md5'], False,'store_true','Calculate MD5 checksum of archive and save to txt file'),
#     (['-md5_verify'], False,'store_true','After calculating the MD5 checksum, read saved file and verify the match'),
#     ]

# for var,v_type,nargs,v_help in positional:
#     parser.add_argument(var, type=v_type, nargs=nargs,help=v_help)

# for var,v_type,nargs,metavar,default,action,v_help in optional:
#     parser.add_argument(*var,type=v_type,nargs=nargs,metavar=metavar,default=default,action=action,help=v_help)

# for var,default,action,v_help in switch:
#     parser.add_argument(*var,default=default,action=action,help=v_help)

# args = parser.parse_args()

# in_dir = args.input_dir[0]
# out_zip = args.output_zip
# if out_zip is None:
#     out_zip = in_dir + '.zip'
# else:
#     out_zip = out_zip[0]
# cpu = args.cpu

# compressor = Blosc(
#     cname=args.compression, 
#     clevel=args.clevel, 
#     shuffle=args.shuffle, 
#     blocksize=args.blocksize
#     )

# verbose = args.verbose
# md5 = args.md5
# md5_verify = args.md5_verify
# if md5_verify:
#     md5 = True

# verbose = args.verbose

# if verbose > 2:
#     print(args)


class alt_zip:
    
    uncompressed_metadata_files = ('compressor.json',)
    
    def __init__(self, archive_location, output_location = None, compressor=None, list_all=None):
        
        self.archive_location = archive_location
        self.list_entries_in_archive()
        
        if list_all:
            print(self.entries)
        
        if compressor is None:
            self.get_compression_metadata()
            self.form_compressor_from_metadata()
        else:
            self.compressor = compressor
        
        self.output_location = output_location
    
    def get_compression_metadata(self):
        
        if 'compressor.json' in self.entries:
            with ZipFile(self.archive_location, 'r') as myzip:
                    with myzip.open('compressor.json') as myfile:
                        self.compressor_json = json.load(myfile)
    
    def form_compressor_from_metadata(self):
        
        if self.compressor_json['id'] == 'blosc':
            self.compressor = Blosc(
                cname=self.compressor_json['cname'], 
                clevel=self.compressor_json['clevel'], 
                shuffle=self.compressor_json['shuffle'], 
                blocksize=self.compressor_json['blocksize']
                )
        
    def list_entries_in_archive(self):
        with ZipFile(self.archive_location, 'r') as myzip:
            self.entries = tuple(myzip.namelist())
        
    def extract(self, output_location=None, files='*'):
        
        if output_location is None:
            buffers = {}
        elif isinstance(output_location, str):
            self.output_location = output_location
        else:
            if self.output_location is None:
                buffers = {}
            else:
                output_location = self.output_location
        
        # assert output_location is not None and isinstance(output_location,str), 'Output location must be defined when extracting archive'
        
        if isinstance(files, str):
            to_get = [files]
        elif isinstance(files, (list,tuple)):
            to_get = files
        elif files == '*':
            to_get = self.entries
        
        print(to_get)
        assert all([x in self.entries for x in to_get]), 'A file is not located in the zip archive'
        
        print(f'Extracting files from archive to location: {output_location}')
        
        with ZipFile(self.archive_location, 'r') as myzip:
            for entry in to_get:
                
                # Read entry
                with myzip.open(entry) as myfile:
                    tmp_file = myfile.read()
                
                # Decompress entry
                print(f'Decompressing {entry}')
                if entry not in self.uncompressed_metadata_files:
                    if self.compressor_json['id'] == 'blosc':
                        tmp_file = self.compressor.decode(tmp_file)
                
                # Return bytes object if file location is not specified
                if output_location is None:
                    buffers[entry] = tmp_file
                else:
                    # Make output file name
                    output_file_name = os.path.join(output_location,entry)
                    
                    # Ensure that directories exist
                    os.makedirs(os.path.split(output_file_name)[0], exist_ok=True)
                    
                    # Write decompressed file to disk
                    with open(output_file_name,'wb') as f:
                        f.write(tmp_file)
        
        if output_location is None:
            if len(buffers) == 1:
                for key, value in buffers.items():
                    return value
            elif len(buffers) > 1:
                return buffers
            elif len(buffers) == 0:
                return None
    
    def __getitem__(self,entry_name):
        '''
        Retrieves an item from zip archive and return bytes
        '''
        return self.extract(output_location=None, files=entry_name)
    
    # def __setitem__(self,entry_name, value):
    #     '''
    #     Adds a filename or bytes object to archive after compression 
    #     '''
        
    #     value_type = None
    #     if isinstance(value, (bytes,BytesIO)):
    #         value_type = 'bytes'
    #     elif os.path.exists(value):
    #         value_type = 'file'
    #     elif value_type is None:
    #         assert False, 'bytes object or filename is required'


# if __name__ == '__main__':
#     start = time.time()
#     # run()
#     compress_dir(in_dir, out_zip, compressor, verbose=verbose, md5=md5, md5_verify=md5_verify)
#     finished = round(time.time()-start,2)
#     print(f'Completed in {finished} seconds')






















