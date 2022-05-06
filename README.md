# Tiramisu
 A file management system with the base-layer-container approach using Docker, Neo4J, and Celery.

## Most Recent Update (02/18/22)   
Tiramisu has been developed into a full, comprehensive Docker service with Celery-powered task manager, React-powered frontend, Neo4J database, Docker volume expoed by Samba protocol, flask-based APi and Nginx web proxy. Automated testing is also integrated. For the moment, the project is private under https://github.com/amarallab/tiramisu until publication. We aim to release this package under open-source. We will update this repository once the project is released.

## Who can use it?
If you are tracking thousands or millions of files that are comprised of logical documents, Tiramisu is for you. Tiramisu is also a good starting foundation to build any file tracker/manager/worker as Neo4J will save relationships, attributes, and children from a file. Tiramisu is also built with non-computational savvy people in mind, as most actions can be done by clicking buttons on the React-based frontend. Tasks can be visualized on a different endpoint powered by Flower. 

Tiramisu is most suitable for running on an isolated cluster/server and have clients connect via SSH or interact with filesystem using Samba. 
