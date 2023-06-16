# compression_tools

## This repository offers tools to assist in file compression

#### Installing:

```bash
# Clone the repo
cd /dir/of/choice
git clone https://github.com/CBI-PITT/compression_tools.git

# Create a virtual environment
# This assumes that you have miniconda or anaconda installed
conda create -n compression_tools python=3.8 -y

# Activate environment and install zarr_stores
conda activate compression_tools
pip install -e /dir/of/choice/compression_tools
```



##### <u>compress_dir:</u>

###### Description:

This tool enables the user to compress a single directory recursively into a .zip file using the Blosc library for compression. By default ZSTD, clevel=5, BITSHUFFLE used as the compressor. Options are available to checksum the the resulting archive (-md5), and validate that the file written to disk matches the expected checksum (-md5_verify). Each archive stores a compression.json file that describes the compression method used. In addition is md5 is selected, a separate .json file will be written which contains the checksum and, if selected, whether the verification passed (-md5_verify)

```bash
python /dir/of/choice/compression_tools/compression_tools/compress_dir.py --help

usage: compress_dir.py [-h] [-out OUT] [-cpu C] [-cmp CMP] [-cl CLV] [-sh SHF] [-bk BLK] [-v] [-md5] [-md5_verify] input_dir [input_dir ...]

Recursively combine a directory into a ZIP file using configurable compression

positional arguments:
  input_dir             One input directorory.

optional arguments:
  -h, --help            show this help message and exit
  -out OUT, --output_zip OUT
                        Output ZIP file name
  -cpu C                Number of cpus which are available
  -cmp CMP, --compression CMP
                        Compression method from Blosc library (zstd (default),blosclz, lz4, lz4hc, zlib or snappy)
  -cl CLV, --clevel CLV
                        Compression level : Integer 0-9 (default 5)
  -sh SHF, --shuffle SHF
                        Shuffle option integer: NOSHUFFLE (0), SHUFFLE (1), BITSHUFFLE (2) or AUTOSHUFFLE (-1) (default 1)
  -bk BLK, --blocksize BLK
                        The requested size of the compressed blocks. If 0 (default), an automatic blocksize will be used
  -v, --verbose         Verbose output : additive more v = greater level of verbosity
  -md5                  Calculate MD5 checksum of archive and save to txt file
  -md5_verify           After calculating the MD5 checksum, read saved file and verify the match

```

###### Usage example:

```bash
python /dir/of/choice/compression_tools/compression_tools/compress_dir.py /directory/to/be/compressed /output/filename.zip -md5 -md5_verify -vv
    
# Output files:
# /output/filename.zip
# /output/filename.zip.md5.json
```



