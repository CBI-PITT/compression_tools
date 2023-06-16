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

import argparse
import psutil
psutil.virtual_memory()

parser = argparse.ArgumentParser(description='''
                                 Recursively combine a directory into a ZIP
                                 file using configurable compression
                                 ''')

positional = [
    ('input_dir',str,'+','One input directorory.'),
    ]

optional = [
    
    (['-out','--output_zip'],str,1,'OUT',None,'store','Output ZIP file name'),
    (['-cpu'],int,1,'C',os.cpu_count(),'store','Number of cpus which are available'),

    # Compression options
    (['-cmp','--compression'],str,1,'CMP','zstd','store','Compression method from Blosc library (zstd (default),blosclz, lz4, lz4hc, zlib or snappy)'),
    (['-cl','--clevel'],int,1,'CLV',5,'store','Compression level : Integer 0-9 (default 5)'),
    (['-sh','--shuffle'],int,1,'SHF',1,'store','Shuffle option integer: NOSHUFFLE (0), SHUFFLE (1), BITSHUFFLE (2) or AUTOSHUFFLE (-1) (default 1)'),
    (['-bk','--blocksize'],int,1,'BLK',0,'store','The requested size of the compressed blocks. If 0 (default), an automatic blocksize will be used'),
    ]

switch = [
    (['-v', '--verbose'], 0,'count','Verbose output : additive more v = greater level of verbosity'),
    (['-md5'], False,'store_true','Calculate MD5 checksum of archive and save to txt file'),
    (['-md5_verify'], False,'store_true','After calculating the MD5 checksum, read saved file and verify the match'),
    ]

for var,v_type,nargs,v_help in positional:
    parser.add_argument(var, type=v_type, nargs=nargs,help=v_help)

for var,v_type,nargs,metavar,default,action,v_help in optional:
    parser.add_argument(*var,type=v_type,nargs=nargs,metavar=metavar,default=default,action=action,help=v_help)

for var,default,action,v_help in switch:
    parser.add_argument(*var,default=default,action=action,help=v_help)

args = parser.parse_args()

in_dir = args.input_dir[0]
out_zip = args.output_zip
if out_zip is None:
    out_zip = in_dir + '.zip'
else:
    out_zip = out_zip[0]
cpu = args.cpu

compressor = Blosc(
    cname=args.compression, 
    clevel=args.clevel, 
    shuffle=args.shuffle, 
    blocksize=args.blocksize
    )

verbose = args.verbose
md5 = args.md5
md5_verify = args.md5_verify
if md5_verify:
    md5 = True

verbose = args.verbose

if verbose > 2:
    print(args)


def compress_dir(in_dir, out_zip, compressor, verbose=0, md5=False, md5_verify=False):
    
    compressor = compressor
    compressor_config = compressor.get_config()
    
    directory_to_compress = glob.glob(in_dir)
    directory_to_compress = [x for x in directory_to_compress if os.path.isdir(x)]
    assert len(directory_to_compress) == 1, 'Only 1 directory can be compressed at a time'
    
    all_files = glob.glob(directory_to_compress[0] + '/**/*', recursive=True)
    all_files = [x for x in all_files if os.path.isfile(x)]
    
    
    def read_bytes(filename):
        with open(filename, 'rb') as f:
            return f.read()
    
    def compress_bytes(byte_string, compressor):
        return compressor.encode(byte_string)
    
    
    def read_and_compress(filename, compressor):
        bytes_string = read_bytes(filename)
        return compress_bytes(bytes_string, compressor)
    
    to_compress = []
    for file in all_files:
        rel_path = os.path.relpath(file,in_dir)
        to_process = (
            rel_path,
            delayed(read_and_compress)(file, compressor)
                      )
        if verbose > 1:
            print(f'Queueing {rel_path}')
        to_compress.append(to_process)
    
    if verbose == 1:
        print('Computing compression')
    to_compress = dask.compute(to_compress)[0]
    # Writing to zip must be sequential which hits the disk with many small writes
    # Here we write each file in a file like object in RAM then make 1 contigious 
    # write to disk.
    zip_stream = BytesIO()
    with ZipFile(zip_stream, 'w') as myzip:
        # Write json metadata the describes compression method
        if verbose == 1:
            print('Writing compressor information')
        myzip.writestr('compressor.json',json.dumps(compressor_config, indent = 4))
        
        # As compression completes write results to zip file
        for result in to_compress:
            if result[0] is not None:
                if verbose > 1:
                    print(f'Writing {result[0]}')
                myzip.writestr(result[0],result[1])
                
    zip_stream.seek(0)
    if verbose == 1:
        print(f'Writing file {out_zip}')
    with open(out_zip, 'wb') as f:
        f.write(zip_stream.getvalue())
    
    
    if md5:
        md5_json = {}
        # Compute MD5
        if verbose == 1:
            print('Computing MD5 Checksum')
        zip_stream.seek(0)
        readable_hash = hashlib.md5(zip_stream.getbuffer()).hexdigest()
        md5_json['md5'] = readable_hash
        if verbose > 1:
            print(readable_hash)
        
        if md5_verify:
            if verbose == 1:
                print('Verifying MD5')
            # Read file and compute md5
            with open(out_zip, 'rb') as f:
                readable_hash_file = hashlib.md5(f.read()).hexdigest()
            
            # Verify hash against origional file
            passed = False
            if readable_hash_file == readable_hash:
                message = 'MD5 Checksum verification: PASSED'
                passed = True
                if verbose == 1:
                    print(message)
            else:
                message = 'MD5 Checksum verification: FAILED'
                if verbose == 1:
                    print(message)
            
            md5_json['verification'] = passed
            
        # Write md5 json file
        with open(out_zip + '.md5.json','w') as f:
            f.write(json.dumps(md5_json, indent = 4))


if __name__ == '__main__':
    start = time.time()
    # run()
    compress_dir(in_dir, out_zip, compressor, verbose=verbose, md5=md5, md5_verify=md5_verify)
    finished = round(time.time()-start,2)
    print(f'Completed in {finished} seconds')






















