Changelog
==========================
v1.2.6
--------------------------
- Add AOSO plugin
- Fix a bug where only first plugin was getting called

v1.2.5
--------------------------
- Add additional notifications about what is happening so users know why there is a slight delay
- Optimize file system reads to speed up usage of file lists

v1.2.4
--------------------------
- Fix problem in remove phase regarding detection of diffs in output files

v1.2.3
--------------------------
- Fix date processing to include 6 digit dates from prior to 2000
- Add changelog

v1.2.2
--------------------------
- Sort deletion list files
- Fix logical problems in working with `.orig` and `.edit` files

v1.2.1
--------------------------
- Fix subversion problem with release

v1.2.0
--------------------------
- Fix bug in facility detection
	- Only 2 character facilities were being detected
- Optimize tar file inspection
- Add new `get_tar_structure` function
- Optimized functions related to tar functionality

v1.1.14
--------------------------
- Fix logging bug

v1.1.13
--------------------------
- Fix deadlock caused by `subprocess.Popen` sending `STDOUT` to `PIPE`

v1.1.12
--------------------------
- Added support for `--ingest-flags`
- Update Readme for better clarification

v1.1.11
--------------------------
- Added support for `tcsh` shell

v1.1.10
--------------------------
- Fix subversion release problem

v1.1.9
--------------------------
- Added version option to APM

v1.1.8
--------------------------
- Subversion release issue

v1.1.7
--------------------------
- Added a plug-in for the `MFRAAF`

v1.1.6
--------------------------
- Fix code preventing ingest from running

v1.1.5
--------------------------
- Fix bug in moving `.edit` files to the `other_files` directory
- Fix bug in moving duplicate files to the `other_files` directory
- Fix bug in passing internal data when using the `--datastream` option
- Fix detection of duplicate files for `datastream` data vs. `SIF` data
- Added ability to change ingest via plug-in
- Added plug-in for `irt` and `irthr` ingest

v1.1.4
--------------------------
- Add check to see if database password for given alias is correct

v1.1.3
--------------------------
- Fix logical errors in `remove` command

v1.1.2
--------------------------
- Add logic to move `.edit` files to `other_files` directory

v1.1.1
--------------------------
- Fix ingest path for production
- Fix bug where > 2 character facility is passed incorrectly to the ingest
- Readme Updates
- Removed old files

v1.0.4
--------------------------
- Add demo capability

v1.0.0
--------------------------
- Inital Release
