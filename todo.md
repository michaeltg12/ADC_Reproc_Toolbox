Find tar with duplicate data
--------------------------------------------
Write a script to go through all data and look or duplicates
Find a tar file with duplicate data
Run the script to to find duplicate data

Cancel run on duplicate detection
---------------------------------
Make apm detect duplicates and save duplicate detected flag
Force cleanup detected before running (this needs to go in apm not stage)
If duplicate detected skip normal stage and run rename


Notes
-----
Have Stage run and not rename the files
If it detects a duplicate file, throw a flag
Once stage is done, APM will then call rename automatically if no duplicates were found
If duplicates are found save flag and exit
If duplicate flag deteted on stage run skip stage and run rename after checking for duplicates being fixed


