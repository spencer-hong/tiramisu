import yaml
from pathlib import Path 
import anytree
import git


__author = 'Spencer Hong'
__date = '01/20/2022'


class Tiramisu:
	"""Represents the main object that will contain base and layers. Synonymous to a Git repository. 
	The main class to query parent-child relationships, databases, and layer branches.

	Tiramisu classes must be instantiated for all other operations in Tiramisu to proceed. 
	"""

	# file extensions that we validate. any other file type we ingest but do not validate.
	ALLOWED_FILES = []

	def __init__(self, baseDir: str, name: str = '', description: str = '', blackList: list = []):
		"""Create a new Tiramisu instance
        :param baseDir:
            The path to either the root git directory or the bare git repo::
                tiramisu = Tiramisu("/Users/tiramisu_baker/tiramisu_examples/archive")
            - Absolute or relative paths are allowed
        :type baseDir: str
        :param name:
            The name assigned to this archive/Tiramisu. This name only has to be provided at the 
            time of assignment.
        :type name: str, optional
        :param description:
            The description assigned to this archive/Tiramisu.
        :type name: str, optional
        :param blackList:
        	List of directories (in string) that will be skipped for digestion. Note that these directories have to absolute paths
        	or relative to the top of this Tiramisu.
        :type blackList: list, optional
        :return: tiramisu.Tiramisu """

		# this is where the /hierarchy, /files and /databases will live

		assert (self.baseDir).exists()

		self.baseDir = Path(baseDir)
		self.description = description
		self.blackList = blackList
		self.name = name
		self.repo = git.Repo(baseDir)

		if not self.repo.bare:
		# try:
		# 	sh.test('-d', (self.baseDir / '.git').as_posix())

			with open(self.baseDir / 'baseMetadata.yaml', 'w') as f:
			    try:
			        baseMetadata = yaml.safe_load(stream)
			    except yaml.YAMLError as exc:
			        print('Error with reading Metadata. ABORT')

			# make sure the .git lives in the same directory as initially specified
			assert self.baseDir.resolve() == baseMetadata['_baseDir']

			assert (self.baseDir / 'hierarchy').exists() == True



		else:
			baseMetadata = {
			    '_name': self.name,
			    '_description': self.description,
			    '_validFileTypes': ALLOWED_FILES,
			    '_blackList': self.blackList,
			    '_baseDir': self.baseDir.resolve()
			}

			with open(Path('baseMetadata.yaml'), 'w') as outfile:
    			yaml.dump(baseMetadata, outfile, default_flow_style=False)

    		assert (self.baseDir / 'hierarchy').exists() == False

    		(self.baseDir / 'files').mkdir(parents = False)
    		(self.baseDir / 'hierarchy').mkdir(parents = False)
    		(self.baseDir / 'databases').mkdir(parents = False)

    		self.repo = self.repo.init(baseDir)

    		self.repo.add(all = True)

    		# now ready for the first commit after digestion


	def digest(self, sourceDir: str, allowedFiles: list = ALLOWED_FILES):

		# call the old tiramisu digest

		# modify the digest function so that /hierarchy have "empty" files (.folder, .file)

		# /files will contain the fileA/fileA.pdf

		# dictionary to save all digested files and their information
        file_df_saved = {}

        # first container is always the root container
        
        # this variable keeps track of the folders that base files are in

        counter = 1

        assert (/yaml exists)

        start_depth = os.path.join(sourceDir, '').count('/')

        rootNode = anytree.AnyNode(id = 'ROOT', fileExtension = '.folder')


        for root, dirs, files in os.walk(sourceDir):
        	for directory in dirs:

        		depth = os.path.join(root, directory).count('/') - start_depth

        		node_id = utils.assign_node_id(depth, root, directory)

            	parent_id = utils.get_parent_id(depth, root, directory)

            	file_df_saved[0] = {'containerID': node_id, 'name': directory, 'layer': 'base',
                                  'fileExtension': '.folder',
                                  'originalPath': os.path.join(root, directory),
                                  'relativePath': self.baseDir / 'files' / node_id
                                  'parentID': parent_id, 'hash': '', 'time': datetime.now()}
                counter += 1

                if depth == 0:
                	tempNode = anytree.AnyNode(id = node_id, fileExtension = '.folder', parent = rootNode)
                else:
                	tempNode = anytree.AnyNode(id = node_id, fileExtension = '.folder', parent = tempNode)

            for file in files:

            	if root in self.blackList:
            		continue

            	depth = os.path.join(root, file).count('/') - start_depth

            	node_id = utils.assign_node_id(depth, root, file)
            	parent_id = utils.get_parent_id(depth, root, file)

            	if depth == 0:
                	tempNode = anytree.AnyNode(id = node_id, fileExtension = '.folder', parent = rootNode)
                else:
                	tempNode = anytree.AnyNode(id = node_id, fileExtension = '.folder', parent = tempNode)






	def create_layer(self, layerDescription: str, layerDatabase, pipelineID: int):

		# layer-specific Databases get saved in /Database
		# only the folders that saw changes will get added for commit 
		# we need to shift the files that became parents 



	def delete_layer(self, pipelineID: int):

		# resets to the commmit right before the layer that will be deleted

	def invalidate_layer(self, pipelineID: int):

		# reverts to the commit right before the layer that will be deleted
		# note that we save this as a commit so we can "undo" this action

	def digest_extend(self):

		# add more files to the "base" layer 
		# files have to be added directly to /hierarchy and /files
		# more of a debugging function, any significant changes should not be anticipated
		# we work/anticipate a pretty much fixed set of data

	def return_layers(self):

		# returns order of layers and their corresponding pipelineIDs & descriptions


def return_file_phases(tiramisu: Tiramisu, originalFilePath: str, fileID: str = None):

	# returns file phases/commits given a fileID or originalFilePath

def return_layer_orders(tiramisu: Tiramisu):

	# returns the layer order

def return_summary(tiramisu: Tiramisu):

	# returns the summary of current Tiramisu 
