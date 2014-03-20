FileSplitter
============

Sequential Splitters
---------------------

**RegexSplitter** : Splits num_of_records of matching Regex patterns.

**XMLSplitter** : Splits num_of_records of XML objects by matching starttag and endtag. 

**LineSplitter** : Marla Line Splitter

Block Splitters
----------------

**AsyncFileSplitter** : Splits file into block_size+bytes_to_next_newline blocks. Does async reads to source file , copies content into memory and  creates split.

**BlockLineSplitter** :  Splits file into block_size+bytes_to_next_newline blocks. Memory maps file , loads content into memory and creates split.

**ZeroCopyLineSplitter** : Splits file into block_size+bytes_to_next_newline blocks. Creates splits without copying source file contents into memory.

**BlockSplitter** : Splits file into blocks.
