from __future__ import absolute_import
from __future__ import print_function

import subprocess
from distutils.command.build import build as _build

import setuptools

class build(_build):
	sub_commands = _build.sub_commands + [('CustomCommands', None)]


CUSTOM_COMMANDS = [
	['apt-get', 'update'],
	['pip3', 'install', 'apache-beam[gcp]==2.15.0'],
	['pip3', 'install', 'google-cloud-storage==1.19.0'],
	['echo', 'DB - Installations done']
]

class CustomCommands(setuptools.Command):
	"""A setuptools Command class able to run arbitrary commands."""

	def initialize_options(self):
		pass

	def finalize_options(self):
		pass

	def RunCustomCommand(self, command_list):
		print('DB - Running command: %s' % command_list)
		p = subprocess.Popen(
			command_list,
			stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
		# Can use communicate(input='y\n'.encode()) if the command run requires
		# some confirmation.
		stdout_data, _ = p.communicate()
		print('DB - Command output: %s' % stdout_data)
		if p.returncode != 0:
			raise RuntimeError(
				'DB - Command %s failed: exit code: %s' % (command_list, p.returncode))

	def run(self):
		for command in CUSTOM_COMMANDS:
			self.RunCustomCommand(command)

REQUIRED_PACKAGES = ['apache-beam[gcp]==2.15.0','google-cloud-storage==1.19.0'] #Not a good practice to specify the versions, makes difficult to upgrade

setuptools.setup(
    name='salecounttracker', #'core',
    version='0.0.1',
    description='Date-wise count of sales.',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(exclude=[".gitignore"]),
    cmdclass={
        # Command class instantiated and run during pip install scenarios.
        'build': build,
        'CustomCommands': CustomCommands,
    }
)
