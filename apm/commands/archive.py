#!/apps/base/python3/bin/python3

import os
import sys
import pwd
import json
import shutil

from datetime import datetime
from glob import glob

from subprocess import Popen
from subprocess import PIPE
from subprocess import CalledProcessError


from apm.pmanager.manager import PluginManager

from apm.classes.db import DB
from apm.classes.ui import UI

from apm.classes.system import convert_date_to_timestamp
from apm.classes.system import dir_pattern
from apm.classes.system import is_number
from apm.classes.system import jprint
from apm.classes.system import get_shell
from apm.classes.system import update_env
from apm.classes.system import update_archive
from apm import Files

############################################################
# Set Development flag
############################################################
global DEVEL
DEVEL = False
############################################################
# status = {
# 	'archive': {
# 		'status': False,
# 		'files_deleted': False,
# 		'move_files': False,
# 		'files_released': False,
# 	}
# }

class Archive:
	""" Send the newly processed files to the archive """

	def __init__(self, config, files=None):
		""" Initialize with args """
		global DEVEL
		DEVEL = config['devel']

		self.config = config
		self.files = files
		self.manager = PluginManager()


	def run(self):
		""" Run the archive portion of the cleanup phase """
		if not self.config['cleanup_status']['remove']['status']:
			print(self.config['cleanup_status']['remove']['status'])
			print('')
			print("Data files must be requested for deletion before the files can be archived.")
			self.config['exit'] = True
			return self.config, self.files

		# Setup vars
		stage = self.config['stage']
		job = self.config['job']


		############################################################
		# Check to see if the current user is `dsmgr`
		############################################################
		# Verify current user is authenticated to run this command
		if not self.authenticate():
			self.config['exit'] = True
			return self.config, self.files

		# Do this if the files have not yet been verified as deleted from the archive
		if not self.config['cleanup_status']['archive']['files_deleted']:
			print("Verifying all files have been deleted from the archive...",end="")
			############################################################
			# Update the local archive database
			############################################################
			# Setup the datastreams to update
			datastreams = []
			datastream_path = dir_pattern(3).format(stage, job, 'datastream')
			for site in os.listdir(datastream_path):
				path = dir_pattern().format(datastream_path, site)
				for folder in os.listdir(path):
					abs_folder = dir_pattern().format(path,folder)
					if os.path.isdir(abs_folder) and not os.path.islink(abs_folder):
						datastreams.append(folder)


			# Update the local copy of the archive db
			if not DEVEL:
				update_archive(datastreams)

			############################################################
			# Load the list of files to be removed from the archive
			############################################################
			deleted_files = []
			deletion_file = dir_pattern(3).format(stage, job, "%s.deletion-list.txt" % job)
			if not os.path.exists(deletion_file):
				print("Failed")
				print("Deletion list does not exist. Please create it and try again.")
				self.config['exit'] = True
				return self.config, self.files

			fp = open(deletion_file, 'r')
			deletion_text = fp.readlines()
			fp.close()

			for line in deletion_text:
				if line.endswith("\r\n"):
					line = line[:-2]

				tar = {}
				parts, tar['md5'] = line.split(' ')
				parts = parts.split('.')
				tar['version'] = parts[-1][1:]
				tar['name'] = '.'.join(parts[:-1])
				deleted_files.append(tar)

				del tar, parts

			if 'line' in locals():
				del line

			############################################################
			# Verify all files have been removed from the archive
			############################################################
			# Get a list of files that are currently at the archive
			archive_files = {}
			db_file = '/apps/ds/conf/datainv/.db_connect'
			alias = 'inv_read'

			db = DB(self.config, db_file=db_file, alias=alias)

			# Store the query
			query = "SELECT * FROM get_remote_files_by_tag('%s') WHERE file_stamp >= %d AND file_stamp <= %d AND file_active = true ORDER BY file_stamp, file_version;"

			# List the column names so the values can be mapped in a dictionary
			cols = ['file_tag', 'file_name', 'file_version', 'file_size', 'file_stored', 'file_md5', 'file_stamp', 'file_checked', 'file_active']

			# convert the start and end dates to a unix timestamp
			start = convert_date_to_timestamp(self.config['begin'])
			end = convert_date_to_timestamp(self.config['end'])

			# Query the database for each of the datastreams
			for k,v in enumerate(datastreams):
				args = (v, start, end)
				result = db.query(query % args, columns=cols)

				if len(result) > 0:
					archive_files[v] = result
				else:
					print("Failed")
					print("No results for %s" % v)

			# Store the list of what is currently in the archive and their versions to file
			current_archive = dir_pattern(3).format(stage, job, 'current_archive.json')
			fp = open(current_archive, 'w')
			fp.write(json.dumps(archive_files, indent=2, sort_keys=False, separators=(',', ': ')))
			fp.close()
			del fp

			if DEVEL:
				file_path = dir_pattern(3).format(stage, job, '%s.archive.json' % job)
				if os.path.exists(file_path):
					fp = open(file_path, 'r')
					archive_files = json.loads(fp.read())
					fp.close()

					del fp, file_path

			# Check to see if any of the "deleted_files" are in the list
			# If yes, quit
			# If no, proceed
			all_files_deleted = None

			if len(deleted_files) > 0:
				# Check the list of files from the archive to see if the current file has been deleted
				for f in deleted_files:
					process = '.'.join(f['name'].split('.')[0:2])
					name = f['name']

					if any(d['file_name'] == name for d in archive_files[process]):
						all_files_deleted = False
						print("Failed")
						print("Not all files have been deleted from the archive.")
						print("Please try again later.")
						self.config['exit'] = True
						return self.config, self.files

				else:
					all_files_deleted = True

			else:
				all_files_deleted = True

			if 'f' in locals():
				del f
			if 'process' in locals():
				del process


			if all_files_deleted != True:
				print("Failed")
				print("Not all files have been removed from the archive.")
				print("Run this again once all files have been removed from the archive.")
				self.config['exit'] = True
				return self.config, self.files

			# Files have been deleted
			self.config['cleanup_status']['archive']['files_deleted'] = True
			print("Done")

		############################################################
		# Move any files not being archived to subdirectories
		#
		# Processed files:
		# This includes any processed files outside the
		# 	date range specified
		# Raw/Tar files:
		# This includes any files that do not need to be rearchived
		############################################################
		if not self.config['cleanup_status']['archive']['move_files']:
			print("Moving files that should not be archived...",end="")


			cwd = os.getcwd()
			datastream = dir_pattern(3).format(stage, job, 'datastream')

			# Load the list of tar files that need to be archived
			os.chdir(dir_pattern().format(stage, job))
			fp = open('archive.json','r')
			contents = json.loads(fp.read());
			fp.close()
			tar_archive = {}
			for k,v in enumerate(contents):
				s = v['site']
				p = v['instrument']
				if s not in tar_archive:
					tar_archive[s] = {}
				if p not in tar_archive[s]:
					tar_archive[s][p] = []

				tar_archive[s][p].append(v['file_name'])

			if len(contents) > 0:
				del s,p,k,v

			os.chdir(datastream)
			sites = os.listdir(datastream)
			for i,s in enumerate(sites):
				os.chdir(s)
				processes = os.listdir('.')
				for j,p in enumerate(processes):
					no_archive = dir_pattern(4).format(datastream, s, p, 'no_archive')
					os.chdir(p)

					if p.split('.')[-1] == '00':
						# This is a raw datastream
						# Don't include directories

						# Get a list of non-tar files from the raw datastreams
						# Move all of these files to a sub-directory
						rawfiles = [x for x in os.listdir('.') if not x.endswith('tar') if not os.path.isdir(x)]

						# Get a list of all tar files from the raw datastreams
						# Retrieve the list of tar files that need to be archived
						# Move all of the files not in the list to a sub-directory
						tarfiles = [x for x in glob("*.tar") if not os.path.isdir(x)]

						for x in rawfiles:
							if not os.path.exists(no_archive):
								os.mkdir(no_archive)
							elif not os.path.isdir(no_archive):
								print("Failed")
								print("There is a file called 'no_archive' in %s.")
								print("This file must be removed before proceeding.")
								self.config['exit'] = True
								return self.config, self.files

							src = dir_pattern(4).format(datastream, s, p, x)
							try:
								os.rename(src, no_archive)
							except OSError:
								shutil.move(src, no_archive)

						for x in tarfiles:
							if not os.path.exists(no_archive):
								os.mkdir(no_archive)
							elif not os.path.isdir(no_archive):
								print("Failed")
								print("There is a file called 'no_archive' in %s.")
								print("This file must be removed before proceeding.")
								self.config['exit'] = True
								return self.config, self.files

							if s not in tar_archive or p not in tar_archive[s] or x not in tar_archive[s][p]:
								src = dir_pattern(4).format(datastream, s, p, x)
								try:
									os.rename(src, no_archive)
								except OSError:
									shutil.move(src, no_archive)

					else:
						# For each processed datastream
						# Get a list of all the files
						# Move any files that fall outside the specified date range to a sub-directory
						if not os.path.exists(no_archive):
								os.mkdir(no_archive)
						elif not os.path.isdir(no_archive):
							print("Failed")
							print("There is a file called 'no_archive' in %s.")
							print("This file must be removed before proceeding.")
							self.config['exit'] = True
							return self.config, self.files

						# Don't include directories
						files = [x for x in os.listdir('.') if not os.path.isdir(x)]

						timeformat = "%Y%m%d"
						begin = datetime.strptime(str(self.config['begin']), timeformat)
						end = datetime.strptime(str(self.config['end']), timeformat)

						for x in files:
							date = x.split('.')[2]
							filedate = datetime.strptime(date, timeformat)

							if not (filedate >= begin and filedate <= end):
								src = dir_pattern(4).format(datastream, s, p, x)
								try:
									os.rename(src, no_archive)
								except OSError:
									shutil.move(src, no_archive)


					os.chdir('..')
				os.chdir('..')
			os.chdir(cwd)

			print("Done")
			self.config['cleanup_status']['archive']['move_files'] = True
		############################################################
		# Read environment variables
		############################################################
		print("Updating environment variables...",end="")

		env_path = dir_pattern().format(stage, job)

		if not update_env(env_path):
			f = Files(self.config)
			shell = f.get_shell()
			if shell == "bash":
				ext = 'sh'
			else:
				ext = 'csh'

			print("Failed")
			exit("Error: Unable to locate env.%s." % ext)

		print("Done") # Updating Env Vars

		############################################################
		# Ensure `DBCONNECT_PATH` does not point to job `.db_connect` file
		############################################################
		if 'DBCONNECT_PATH' in os.environ:
			del os.environ['DBCONNECT_PATH']

		# The command should be complete up to this point,
		# however I'm waiting on a response to verify the exact name
		# of this environment variable

		############################################################
		# Run `release_data`
		############################################################
		print("Running release_data...",end="")

		#############################################
		# Need to change this so it supports both
		#  `sif` data and `datastream` data
		#############################################
		db = DB(self.config)

		data_paths = db.get_data_paths()

		commands = []

		for d in data_paths:
			output = d['output']
			(site, temp) = output.split('/')
			temp = temp.split('.')[0][3:]
			for i,e in reversed(list(enumerate(temp))):
				if not is_number(e):
					fac = i
					break
			else:
				print("Could not separate facility from %s" % temp)
				self.config['exit'] = True
				return self.config, self.files

			facility = temp[fac:]
			process = temp[:fac]
			command = [
				'release_data',
				'-s',
				site,
				'-f',
				facility,
				process
			]
			# Check to see if a plugin needs to modify the command
			command = self.manager.callPluginCommand('hook_release_data_command_alter', command)
			commands.append(command)

		# code to run a shell command copied from other part of APM
		# Needs modified to work here

		# Run the command
		for command in commands:
			try:
				if not DEVEL:
					ps = Popen(command, stdout=PIPE, stderr=PIPE)
					ps.communicate()
					returncode = ps.returncode
					if returncode != 0:
						print("Failed")
						self.config['exit'] = True
						return self.config, self.files
			except CalledProcessError as e:
				print("Failed")
				self.config['exit'] = True
				return self.config, self.files
			except Exception as e:
				raise e


		print("Done")

		# Files have been released
		self.config['cleanup_status']['archive']['files_released'] = True

		# Archive is complete
		self.config['cleanup_status']['archive']['status'] = True


		return self.config, self.files


	def authenticate(self):
		global DEVEL
		allowed_user = ['dsmgr']
		error = "This portion of APM can only be run by DSMGR."
		username = pwd.getpwuid(os.getuid())[0]

		if DEVEL:
			allowed_user.append('twilliams')

		if username in allowed_user:
			return True
		else:
			print(error)
			return False



