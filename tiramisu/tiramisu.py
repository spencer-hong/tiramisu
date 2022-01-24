import yaml
from pathlib import Path 
from treelib import tree 
import git
import os
import pprint as pp
from pdb import set_trace as bp
from zipfile import ZipFile
from pandas import DataFrame
import utils 
from datetime import datetime

__author = 'Spencer Hong'
__date = '01/20/2022'

# file extensions that we validate. any other file type we ingest but do not validate.
ALLOWED_FILES = ['.pdf', '.doc', '.docx', '.ppt', '.pptx', '.xls', '.xlsx', '.png', '.tiff', '.jpg', '.zip', '.txt', '.csv']

class Tiramisu:
	"""Represents the main object that will contain base and layers. Synonymous to a Git repository. 
	The main class to query parent-child relationships, databases, and layer branches.

	Tiramisu classes must be instantiated for all other operations in Tiramisu to proceed. 
	"""


	def __init__(self, baseDir: str, name: str = '', description: str = '', blackList: list = None):
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



		self.baseDir = Path(baseDir).resolve()

		assert (self.baseDir).exists()
		self.description = description

		if blackList == None:
			self.blackList = ['.git']
		elif '.git' in blackList:
			self.blackList = blackList
		else:
			self.blackList = blackList.append('.git')

		self.name = name
		try:
			self.repo = git.Repo(self.baseDir)

			with open(self.baseDir / 'baseMetadata.yaml', 'r') as f:
				try:
					self.baseMetadata = yaml.safe_load(f)
				except yaml.YAMLError as exc:
					print('Error with reading Metadata. ABORT')

			# make sure the .git lives in the same directory as initially specified
			assert self.baseDir.as_posix() == self.baseMetadata['_baseDir']

			assert (self.baseDir / 'files').exists() == True

			assert (self.baseDir / 'databases').exists() == True

			# self.database = pd.read_parquet(self.baseDir / 'databases' / 'main.parquet')

		except git.exc.InvalidGitRepositoryError:

			self.baseMetadata = {
				'_name': self.name,
				'_description': self.description,
				'_validFileTypes': ALLOWED_FILES,
				'_blackList': self.blackList,
				'_baseDir': self.baseDir.resolve().as_posix()
			}

			with open(self.baseDir / 'baseMetadata.yaml', 'w') as outfile:
				yaml.dump(self.baseMetadata, outfile, default_flow_style=False)

			assert (self.baseDir / 'files').exists() == False

			(self.baseDir / 'files').mkdir(parents = False)
			(self.baseDir / 'databases').mkdir(parents = False)
			utils.write_gitignore(self.baseDir)

			self.repo = git.Repo.init(baseDir)

			self.repo.git.add(all = True)

			self.repo.index.commit('Preprocessing Base Layer. IGNORE.')

			# self.database = None

			# now ready for the first commit after digestion

		if self.baseMetadata['_blackList'] == None:
			self.blackList = ['.git']


	def digest(self, sourceDir: str, allowedFiles: list = ALLOWED_FILES):

		# call the old tiramisu digest

		# modify the digest function so that /hierarchy have "empty" files (.folder, .file)

		# /files will contain the fileA/fileA.pdf

		# dictionary to save all digested files and their information
		file_df_saved = {}

		rootTree = tree.Tree()

		# first container is always the root container
		
		# this variable keeps track of the folders that base files are in

		folderFiles = []
		zipFiles = []

		counter = 1

		start_depth = os.path.join(sourceDir, '').count('/')

		rootTree.create_node(sourceDir, 'ROOT')


		for root, dirs, files in os.walk(sourceDir, topdown = True):
			dirs[:] = [d for d in dirs if d not in self.blackList]
			dirs[:] = [d for d in dirs if d not in self.repo.ignored(dirs)]
			files[:] = [f for f in files if f not in self.repo.ignored(files)]
			for directory in dirs:

				
				depth = os.path.join(root, directory).count('/') - start_depth

				node_id = utils.assign_node_id(depth, root, directory)

				parent_id = utils.get_parent_id(depth, root, directory)


				rootTree.create_node(directory, node_id, parent_id)

				utils.make_folder(self.baseDir / 'files'/ parent_id)

				directory = Path(directory)

				tiramisuPath = (self.baseDir / 'files'/ parent_id / node_id ).as_posix()+ '.folder'

				with open(tiramisuPath, 'w') as f:
					pass

				folderFiles.append(node_id+ '.folder')

				file_df_saved[counter] = {'containerID': node_id, 'name': directory.name, 'layer': 'base',
								  'fileExtension': '.folder',
								  'originalPath': directory.resolve().as_posix(),
								  'tiramisuPath': tiramisuPath,
								  'parentID': parent_id, 'hash': '', 'time': datetime.now()}
				counter += 1

			for file in files:

				if root in self.blackList:
					continue
				if file in folderFiles:
					continue

				depth = os.path.join(root, file).count('/') - start_depth

				file = Path(file)

				node_id = utils.assign_node_id(depth, root, file.stem)
				parent_id = utils.get_parent_id(depth, root, file.stem)

				rootTree.create_node(file.stem, node_id, parent_id)

				utils.make_folder(self.baseDir / 'files' / parent_id)

				tiramisuPath = (self.baseDir / 'files'/ parent_id / node_id).as_posix() + file.suffix

				utils.copy_files(root / file, tiramisuPath)

				correctedFilePath = utils.verify_file_type(Path(tiramisuPath))

				if correctedFilePath.suffix == '.zip':
					zipFiles.append((node_id, tiramisuPath))

				file_df_saved[counter] = {'containerID': node_id, 'name': correctedFilePath.name, 'layer': 'base',
								  'fileExtension': correctedFilePath.suffix,
								  'originalPath': (root / file).resolve().as_posix(),
								  'tiramisuPath': correctedFilePath.resolve().as_posix(),
								  'parentID': parent_id, 'hash': utils.generate_hash(correctedFilePath.as_posix()), 'time': datetime.now()}
				counter += 1

				utils.lock_files_read_only(correctedFilePath)

		while len(zipFiles) != 0:

			utils.make_folder(self.baseDir / 'files' / 'tmp')

			zipDir = self.baseDir / 'files'/ 'tmp' / zipFiles[0][0]

			utils.make_folder(zipDir)

			start_depth = os.path.join(zipDir, '').count('/')

			with ZipFile(zipFiles[0][1], 'r') as zipRef:

				listOfFileNames = zipRef.namelist()

				for zipFile in listOfFileNames:
					# __MACOSX is a artifact of unzipping in MacOS
					if not '__MACOSX' in zipFile:
						zipRef.extract(zipFile, zipDir) 

			for root, dirs, files in os.walk(zipDir, topdown = True):
				dirs[:] = [d for d in dirs if d not in self.blackList]
				dirs[:] = [d for d in dirs if d not in self.repo.ignored(dirs)]
				files[:] = [f for f in files if f not in self.repo.ignored(files)]
				for directory in dirs:

					depth = os.path.join(root, directory).count('/') - start_depth

					if depth == 0:
						parent_id = zipFiles[0][0]
					else:
						parent_id = utils.get_parent_id(depth, root, directory)

					node_id = utils.assign_node_id(depth, root, directory)

					rootTree.create_node(directory, node_id, parent_id)

					assert (self.baseDir / 'files'/ parent_id).exists() == True

					directory = Path(directory)

					tiramisuPath = (self.baseDir / 'files'/ parent_id / node_id ).as_posix()+ '.folder'

					with open(tiramisuPath, 'w') as f:
						pass

					folderFiles.append(node_id+ '.folder')

					file_df_saved[counter] = {'containerID': node_id, 'name': directory.name, 'layer': 'base',
									  'fileExtension': '.folder',
									  'originalPath': directory.resolve().as_posix(),
									  'tiramisuPath': tiramisuPath,
									  'parentID': parent_id, 'hash': '', 'time': datetime.now()}
					counter += 1

				for file in files:

					if root in self.blackList:
						continue
					if file in folderFiles:
						continue

					depth = os.path.join(root, file).count('/') - start_depth

					if depth == 0:
						parent_id = zipFiles[0][0]
					else:
						parent_id = utils.get_parent_id(depth, root, file.stem)

					file = Path(file)

					node_id = utils.assign_node_id(depth, root, file.stem)

					rootTree.create_node(file.stem, node_id, parent_id)

					utils.make_folder(self.baseDir / 'files' / parent_id)

					tiramisuPath = (self.baseDir / 'files'/ parent_id / node_id).as_posix() + file.suffix

					utils.copy_files(root / file, tiramisuPath)

					correctedFilePath = utils.verify_file_type(Path(tiramisuPath))

					if correctedFilePath.suffix == '.zip':
						zipFiles.append((node_id, tiramisuPath))

					file_df_saved[counter] = {'containerID': node_id, 'name': correctedFilePath.name, 'layer': 'base',
									  'fileExtension': correctedFilePath.suffix,
									  'originalPath': (root / file).resolve().as_posix(),
									  'tiramisuPath': correctedFilePath.resolve().as_posix(),
									  'parentID': parent_id, 'hash': utils.generate_hash(correctedFilePath.as_posix()), 'time': datetime.now()}
					counter += 1

					utils.lock_files_read_only(correctedFilePath)
			del zipFiles[0]



		if (self.baseDir / 'hierarchy.txt').exists():
			(self.baseDir / 'hierarchy.txt').unlink()

		rootTree.save2file(self.baseDir / 'hierarchy.txt', line_type = 'ascii-em')

		self.database = DataFrame.from_dict(file_df_saved, orient='index')

		self.database.to_parquet(self.baseDir / 'databases' / 'main.parquet')

		utils.remove_folder(self.baseDir / 'files' / 'tmp')

		return rootTree


	def create_layer(self, layerDescription: str, layerDatabase, pipelineID: int):

		raise NotImplementedError

		# layer-specific Databases get saved in /Database
		# only the folders that saw changes will get added for commit 
		# we need to shift the files that became parents 



	def delete_layer(self, pipelineID: int):

		raise NotImplementedError

		# resets to the commmit right before the layer that will be deleted

	def invalidate_layer(self, pipelineID: int):

		raise NotImplementedError

		# reverts to the commit right before the layer that will be deleted
		# note that we save this as a commit so we can "undo" this action

	def digest_extend(self):

		raise NotImplementedError

		# add more files to the "base" layer 
		# files have to be added directly to /hierarchy and /files
		# more of a debugging function, any significant changes should not be anticipated
		# we work/anticipate a pretty much fixed set of data

	def return_layers(self):

		raise NotImplementedError

		# returns order of layers and their corresponding pipelineIDs & descriptions

	def __repr__(self):
		_toPrint = pp.PrettyPrinter(indent = 4)

		return _toPrint.pformat(self.baseMetadata)

	def __str__(self):
		_toPrint = pp.PrettyPrinter(indent = 4)

		return _toPrint.pformat(self.baseMetadata)


def return_file_phases(tiramisu: Tiramisu, originalFilePath: str, fileID: str = None):

	raise NotImplementedError

	# returns file phases/commits given a fileID or originalFilePath

def return_layer_orders(tiramisu: Tiramisu):

	raise NotImplementedError

	# returns the layer order

def return_summary(tiramisu: Tiramisu):

	raise NotImplementedError

	# returns the summary of current Tiramisu 