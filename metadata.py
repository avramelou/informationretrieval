from os import path
# This file holds information about the file names, paths and also some metadata


DATA_FILENAME = "data_files"
DOCUMENTS_DATA_FILENAME = "documents_data"
INVERTED_INDEXERS_FILENAME = "inverted_indexers"

script_path = path.abspath(__file__)  # full path of the script
dir_path = path.dirname(script_path)  # full path of the directory of your script

DATA_FILES_PATH = path.join(dir_path, DATA_FILENAME)  # absolute data_files file path

BLOCK = "doc_data_block_"

# The name of the file that has the metadata
METADATA_DICTIONARY_FILENAME = "metadata.npy"
# The name of the file that has inverted indexer_dict
INVERTED_INDEXER_FILENAME = "inverted_indexer.npy"
# The name of the file that has the documents metadata
DOC_METADATA_FILENAME = "documents_metadata.npy"
# Prefix of each of the block file that has documents
BLOCK_FILE_NAME_PREFIX = path.join(DOCUMENTS_DATA_FILENAME, BLOCK)

# Complete path for each of the files
# metadata
METADATA_DICTIONARY_FILE_PATH = path.join(DATA_FILES_PATH, METADATA_DICTIONARY_FILENAME)
# inverted indexer_dict
INVERTED_INDEXER_FILE_PATH = path.join(DATA_FILES_PATH, INVERTED_INDEXER_FILENAME)
# documents metadata
DOC_METADATA_FILE_PATH = path.join(DATA_FILES_PATH, DOC_METADATA_FILENAME)
# Prefix of each of the block file that has documents
BLOCK_FILE_NAME_PREFIX_PATH = path.join(DATA_FILES_PATH, BLOCK_FILE_NAME_PREFIX)

# metadata dictionary special keys
META_TOTAL_BLOCKS_KEY = "total_blocks"
META_BLOCK_SIZE_KEY = "max_block_size"
META_CRAWLED_LINKS_SET_KEY = "crawled_links_set"

# Each block will have by default some websites saved in it
DEFAULT_MAX_BLOCK_SIZE = 1000
