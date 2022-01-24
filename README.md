# tiramisu
 A file management system with the base-layer-container approach

Files are digested at the base level. Any edits/changes/expansion of files are saved as layers on top of the base.   
Each file is a container that may contain more than one file. This allows Tiramisu to maintain parent-child relationships and one-to-one & one-to-many relationships. Tiramisu takes advantage of Git as its version control system where each commit is a "layer" on top of the base. Therefore, each tiramisu can be seen as a Git repository. 

This library was created for the archive-digestion project. Tiramisu is the foundation of the overall dockerized pipeline controlled by Luigi.


## Installation

Tiramisu depends on Pandas (parquet management), python-magic (file correction), treelib (hierarchy), gitPython (Git management), and pyYAML (printing). These are all mature libraries that should not break. To install python-magic,  
`brew install libmagic` for OSX   
`sudo apt-get install libmagic1` for Linux  

then follow with `pip install python-magic`

## Known limitations  

| Limitation | Plans to fix it? |
| --------------- | --------------- |
| Unzipping only supports .zip files | Yes |