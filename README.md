# tiramisu
 A file management system with the base-layer-container approach

Files are digested at the base level. Any edits/changes/expansion of files are saved as layers on top of the base.   
Each file is a container that may contain more than one file. This allows Tiramisu to maintain parent-child relationships and one-to-one & one-to-many relationships. 

 Created for the archive-digestion project

| Features | Implemented? | Deadline |
| --------------- | --------------- | --------------- |
| Unzip & de-folder files | Yes | 12/01/21  |
| Serialize structure for future reads| Yes | 12/09/21 |
| Correcting file extensions| Yes | 12/10/21 |
| Saving file hashes| Yes | 12/10/21 |
| Locking files read-only| Yes | 12/10/21 |
| Digest the oldest archive files| No | 12/10/21 |
| Speed tests and benchmarks | No | 12/12/21
| Integrate splitting pages & converting PDFs to images | No | 12/14/21 |
| Integrate splitting of PDF files to pages | No | 12/20/21 | 
| Digest the newest archive files | No | 12/24/21 |
| Obtain archive statistics | No | 01/04/22 |
| Create examples for GitHub repo | No | 01/05/22 |
| Figure out how to modify label studio with tiramisu | No | 01/05/22 |

## Known limitations  

| Limitation | Plans to fix it? |
| --------------- | --------------- |
| Unzipping only supports .zip files | Yes |