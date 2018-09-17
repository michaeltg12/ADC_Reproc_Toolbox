# APM

## Description
This is a command line tool used to automate reprocessing tasks.

## Help Text

```
usage: apm [-h] [-b BEGIN] [-e END] [-s SITE] [-i INSTRUMENT] [-f FACILITY]
              [-d DATASTREAM [DATASTREAM ...]] [-j JOB] [-a ALIAS]
              [--stage STAGE] [--source SOURCE] [--no-rename] [--no-db-up]
              [--no-compare] [--ingest | --vap] -I | -q]
              command

ARM Processing Manager

positional arguments:
  command                     Which of the APM stages to run: stage, rename,
                              process, review, remove, archive, cleanup

optional arguments:
  -h, --help                  show this help message and exit

  -b, --begin BEGIN           Format: YYYYMMDD - date to start processing data
  -e, --end END               Format:YYYYMMDD - date to stop processing data

  -s, --site SITE             The site the data is from
  -i, --instrument INSTRUMENT The instrument used to collect the data
  -f, --facility FACILITY     The facility where the instrument is located
  
  -d, --datastream DATASTREAM One or more datastream patterns. "%" and "*" can be
                              used as wildcards.
  -j, --job JOB               Job name to use to run the job again
  -a, --alias ALIAS           An alias for the Ingest to use to connect to the
                              database. Default: apm
  
  --stage STAGE               Specify a staging directory
  --source SOURCE             Specify a source directory
  --no-rename                 Do not strip the ARM prefix from the files
  --no-db-up                  Do not update the config database
  --no-compare                Do not compare the ingest output for re-archiving

  --ingest-flags INGEST_FLAGS Flags you want APM to pass to the INGEST.
                              Ex. --ingest-flags F
                              (Do not use "-F" APM will add the "-")
                              (Will apply to all ingests if running for
                                multiple datastreams)

  --ingest                    Ingest vs. VAP (default)
  --vap                       VAP vs. Ingest
  -I, --interactive           Prompt for various inputs
  -q, --quiet                 Suppresses prompts and exits gracefully if unable to
                              run
```

----

## Commands:
### Stage
The `stage` command is the first command to be run. With this command you would usually specify all of your parameters to setup the job. The exception to this is if you are rerunning an existing job, or if you already have a `<job>.conf` file.

This command makes all the necessary preparations for the Ingest or VAP to reprocess the specified data, including setting up environment variables.

The environment variables needed are written to the `env.sh` file for the bash or the `env.csh` file for the csh shell. APM determines which file to create based on the default shell of the user running APM.

It is not necessary to run the `env.(c)sh` file manually, since APM will use this file to import the environment variables when needed. This file is provided so the user can run various stages of reprocessing manually if needed.

APM Stage will create a job folder (referred to as <job> in this documentation) it its staging directory (default can be found below in the description of the `stage` option) and will create the necessary directory structure to process data within that project directory.

Stage also takes a copy of the tar files that it unpacks and the raw unpacked files and stores them in the `file_comparison/tar` and `file_comparison/raw` directories respectively.

#### Ingest
For Ingest reprocessing tasks, APM stage takes the parameters provided and determines what raw files it needs to unpack.

APM retrieves tar files from its source location (default: `/data/archive`), and unpacks the tar files into the `<job>/collection` directory. As it unpacks the tar files it is looking for files that either have or will have a naming collision when the file names have been stripped of their ARM prefixes, and adds a suffix to the end of files that would overwrite each other. It then warns the user that there are files with naming collisions and asks the user to choose the correct file before proceeding.

__Warning__: At this time it is APM assumes that you will only need one of the files that have naming collisions, and the others can be discarded. If you need more than one file, APM will complain that the file is not being tracked and you will need to manually edit the `<job>.json` file to track the file. There are plans to update APM in the future, so you will be able to run a command and APM will dynamically track the files for you, but it is not yet implemented.

As stage unpacks the files, it checks to see if the file has a `raw`, `orig`, `bad` or `edit?`(? represents a number, ex. `edit2`) keyword. Raw files are placed in their respective folders, while `orig`, `bad`and `edit?` files are placed in the `other_files` subdirectory.

Once the files are unpacked into the `collection` directory, APM automatically runs the `rename` command defined below.

__Note:__ To ensure that all of the raw files within the specified date range are collected, APM will look for the file with the closest date within the range, and will collect the tar files +- 1 file. This means if the date range given is 3/5 - 3/7 and files are available for every day from 3/1 - 3/10, APM will grab the files from 3/4 - 3/8. If the file for 3/4 does not exist but 3/3 does, then APM will grab the files for 3/3 - 3/8. APM will also grab all of the tar files for the date in question, so if multiple files are available each date, APM will grab all of the files for the dates it is looking for not just the next or previous file.

#### VAP
For VAPs APM stage will write an `env.(c)sh` file with the needed commands to setup the environment and call `vapmgr` to create the necessary symlinks for vapmgr to run the VAP processing later on. These symlinks are not created until the process phase of APM.

### Rename
This command will strip the ARM prefix from all files unpacked into the collection directory. It will update the `<job>.json` file to keep track of the files with their new names. This command is unnecessary to run separately since it is run automatically by the Stage command. It is provided so it can be run manually if that becomes necessary at some point in time.

Example:  
Given the following filename: `sgpmfrsrE9.00.20140604.200000.raw.20140604_200000.dat`  
Rename will strip the ARM prefix with the following resulting filename: `20140604_200000.dat`

### Process
The process command runs the Ingest or VAP needed to process the specified data in the reprocessing task. It determines what Ingest or VAP to run based on the options set in previous commands.

This command does the following things for both Ingest and VAP reprocessing tasks:

* Update the environment variables from the `env.(c)sh` file
* Locates the appropriate `.db_connect` file to determine the appropriate information for either the Ingest or VAP reprocessing run

#### Ingest
For Ingests, APM process goes through the following steps:

* Locate the appropriate ingest executable
* Run the Ingest executable against the data in the job
* Parse the Ingest logs to track the files and their updated names as they are moved from `collection` to `datastream`

Any detected errors are then displayed to the user.

#### VAP
For VAPs, APM process goes through the following steps:

* Run `vapmgr` with the provided parameters
* Check the return code for errors
* Parse the following logs for errors:
  * vap logs
  * vapmgr logs
  * vapmgrqc logs

If an error is detected, the user is notified and told where in the log files to find more information.

### Review
This command is currently unimplemented. It is intended to automate any review tasks (ex. running nc review), but none are known to be able to be automated at this time.

### Remove
This command looks for files that need to be removed from the archive. Once it has determined all of the files that should be removed, it emails the list of files, as an attachment named `<job>.deletion-list.txt`to `dmfoper@arm.gov`.

APM remove uses the following information to determine what files need removed from the archive:

APM compares the raw files post processing to the files unpacked to `file_comparison/raw`. APM then looks for the tar files that contain any raw files that have changed and marks the tar file for deletion. It then bundles the raw data and looks for the new tar files that contain all of the raw files that are being removed from the archive, so they can be added back into the archive later.

For processed files, APM looks checks the archive database for any files in the given date range for the specified process. If any filenames exist that are not in the list of processed files for this job, they are marked for deletion. Files that have the same name as the newly processed files are not removed as they will be over-versioned in the archive.

### Archive
The APM Archive command does the following things:

* Verify the APM remove command has been run successfully (this is done through status flags in the job config file)
* All of the files in the emailed deletion list have been removed from the archive
* Move all files that should not be archived into subdirectories (see paragraph below for how APM determines what files should be archived)
* Read in the environment variables from the `env.(c)sh` file
* Run the `release_data` command (`release_data` must be run by `dsmgr`)

__Note:__ Because `release_data` must be run by `dsmgr`, this command can only be run that user.

__How APM chooses archive files:__

1. Raw files:  
During the remove command APM makes a list of any tar files that need to be archived. This is determined by what tar files are removed from the archive. During archive APM retrieves this list of tar files and moves all other files to the `no_archive` subdirectory. Since `release_data` only looks at files and not directories this is a safe way to make sure the files don't get archived without deleting them.
1. Processed files:  
Files processed by an ingest or VAP are labeled with a correct date stamp. While stage grabs tar files outside the date range specified, the extra files are removed here. All processed files outside of the date range are moved to the `no_archive` subdirectory. All processed files within the date range are left in place.

### Cleanup
This command removes all files not needed historically for the preservation of the job by performing the following steps:

* Verify all files that should be archived have been.  
This is done by verifying that the file no exists in the archive and either did not previously in the archive, or its version number is now higher in the archive than it was previously. This comparison is able to be done by saving the information of what is in the archive during the APM archive command.
* Delete the files or empty the directories from the following list: (directories have a trailing `/`)
  * datastream/
  * collection/
  * file_comparison/raw/
  * file_comparison/tar/
  * archive.json
  * current_archive.json
  * <job>.deletion-list.txt

The above directories and files are not needed for APM to run again from scratch. This does not delete the configuration, or the log files, but decreases the size of the job directory so it can be archived if desired.

__Note:__ If archiving the job directory, the `<job>.conf` file in the job directory is a symlink to the following location `~/.apm/<job>.conf`. You will want to move the original file into the job directory before archiving the directory. If this file is not moved, the job will not actually contain its config file. This is done so the user can run APM for the specified job from any location, not just from within the job directory. This also allows the job directory to be deleted in order to start completely over if a mistake was made.

----
## Options
##### -h, --help
Display the help message.

##### -b BEGIN, --begin BEGIN
Specify a date to start processing data  
Format: YYYYMMDD

##### -e END, --end END
Specify a date to stop processing data  
Format: YYYYMMDD

##### -s SITE, --site SITE
The site from which to gather the data.

##### -i INSTRUMENT, --instrument INSTRUMENT
The instrument used to collect the data.

##### -f FACILITY, --facility FACILITY
The facility where the instrument is located.

##### -d DATASTREAM, --datastream DATASTREAM [DATASTREAM ...]
One or more datastream patterns. "%" and "\*" can be used as wildcards. This option allow the user to specify multiple datastreams, or a datastream pattern (ex. `sgp*C1.00` or `sgpmfrsr*.00`)

This option is mutually exclusive with the -s, -i, -f options. Either the site, instrument and facility, or the datastream should be specified. If the datastream is specified the site, instrument and facility options will be cleared in the config file. If any of the site, instrument and facility options are specified, the datastream option will be cleared in the config file.

See below for examples.

##### -j JOB, --job JOB
A unique name to use as the identifier for the job. This should be the RID for the reprocessing job. This option also specifies the directory in which the project is stored. While this option is not required, it will default to a 12 digit portion of a UUID making it fairly unique, but not as easy to remember, it is recommended to always specify this option.

##### -a ALIAS, --alias ALIAS
A database alias within the `.db_connect` file for the Ingest to use to connect to the database.  
Default: apm

##### --stage STAGE
Allows the user to specify a staging directory other than the default. This is where the job directory will be placed.  
The default location for this directory is chosen by going through the following choices:

1. If `APM_HOME` environment variable is set, this will be used,
2. If the users home directory is located in `/data` the staging directory will be `~/apm`,
3. The staging directory will be `/data/home/<username>/apm` where `<username>` is the name of the user running the command.


##### --source SOURCE
Allows the user to specify a source directory other than the default. This is where the tar files that hold the raw data are located.  
Default: `/data/archive`

##### --no-rename
Do not strip the ARM prefix from the files as they are unpacked.

##### --no-db-up
When the ingest executable is found in the process command, do not update the database configuration to the latest settings. This can be specified if the settings have changed and you do not want to update the database for your current reprocessing task.

##### --no-compare
Do not compare the ingest output for re-archiving. This will skip the bundle process and not re-archive any tar files regardless of whether or not the raw files have changed.
##### --ingest-flags 
Add a list of flags that need to be passed to an ingest. Example:

```
apm -j myJobName process --ingest-flags F
```
This will add the `-F` flag to the end of the ingest command running the ingest in FORCE mode.

**Notes:** 
> Do not put the `-` in front of the flag, APM does this automatically. If you enter `--ingest-flags -F` instead of `--ingest-flags F` the command line will assume the `-F` is meant for APM to analyze instead of pass to the ingest.  

> Because this option can take multiple arguments this option cannot go immediately in front of the positional command option. It must either come after the command argument, or have another option between them. See below:

> OK

```
apm -j myJobName process --ingest-flags F
apm process -j myJobName --ingest-flags F
apm process --ingest-flags F -j myJobName
apm --ingest-flags F -j myJobName process
```

> Not OK

```
apm -j myJobName --ingest-flags F process
apm --ingest-flags F process -j myJobName
```

##### --ingest
Specify to run an ingest reprocessing job (this is the default and does not need to be specified).

##### --vap
Specify to run a VAP reprocessing job.

##### -I, --interactive
Have APM prompt for inputs interactively. This allows the user to input data as APM needs it instead of specifying the data on the command line when running the command. This flag is not sticky, meaning it needs to be specified every time you want to be prompted for data.

##### -q, --quiet
Suppresses prompts and exits gracefully if unable to run. Some settings will prompt the user for data if data is needed regardless of whether or not the interactive flag is selected. This flag will quiet those prompts and not prompt for any data but will exit gracefully if an error occurs.

This option can be used in the event that APM needs to be run via cron or other headless method where a prompt is not available. This flag, like the interactive flag, is not sticky.

----
## Plugins
APM is designed to handle 80% of the situations that arise with reprocessing data. If there is something that APM does not do, but is needed for a specific case, a plugin can be written to handle that case.

----
## Examples:
### Stage
To unpack the tar files between the dates of 2014-05-01 and 2014-05-07 for an mfrsr at sgpC1

```bash
apm stage -b 20140501 -e 20140507 -s sgp -i mfrsr -f C1 -j
```

To save the above run as a job:

```bash
apm stage -b 20140501 -e 20140507 -s sgp -i mfrsr -f C1 -j myJobName
```
The above job can be re-run as follows:

```bash
apm stage -j myJobName
```

This allows the user to rerun the job without having to specify all of the criteria again. Any changes specified with the job on a re-run will be saved in the job config file.  
For example:

```bash
apm stage -j myJobName -b 20140502
```

Will run the job `myJobName` again with the beginning date changed from May 1st to May 2nd. This change will be saved to the job config and subsequent runs will have a starting date of May 2nd, 2014.

### Datastreams
Instead of listing only one instrument at a specific site and facility, multiple processes can be specified using the datastream option.

```bash
apm stage -b 20140501 -e 20140507 -d sgpmfrsrC1.00
```
The above command will run exactly as the previous examples.

This example will run any instrument at the sgpC1 facility

```bash
apm stage -b 20140501 -e 20140507 -d sgp*C1.00
```

And this example will run any mfrsr at any site and facility.

```bash
apm stage -b 20140501 -e 20140507 -d *mfrsr*.00
```

Multiple datastreams can also be listed. This example will get all sgp mfrsr and aos processes.

```bash
apm stage -b 20140501 -e 20140507 -d sgpmfrsr*.00 sgpaos*.00
```

### Running APM
The `command` option is a positional argument. This means if there were other positional arguments, APM would know which one was which by the order in which they are specified. The placement of optional arguments before or after the command option does not affect it, except for the `-d` option. Since the `-d` option can take any number of inputs, a positional argument cannot come directly after it, but should be placed before it, or have another optional argument between them.  
The following examples illustrate:


This example will run

```bash
apm stage -d sgpmfrsrC1.00
```
This example will not run because APM will think the `stage` option is an input to the `-d` argument

```bash
apm -d sgpmfrsrC1.00 stage
```
This example will run because an additional option is specified between the `-d` and the `stage` designation for the `command` argument

```bash
apm -d sgpmfrsrC1.00 -j myJobName stage
```
### Other Command Examples
Following are examples of how to run each of the other commands. Note that none of the options other than the job name need to be specified since they are stored in the job configuration file.

```bash
apm process -j myJobName
apm remove -j myJobName
apm archive -j myJobName
apm cleanup -j myJobName
```
__Note:__ `rename` and `review` commands are not shown in the examples since rename is run by `stage` and `review` is not yet implemented at this time.

----
## To Do:
* Add a way for users to have a file renamed or deleted when a naming collision is detected. This method should also update the `<job>.json` file to keep file tracking up to date. More information can be found on this [Trello Card][apm_mv]
* Move all miscellaneous 
* Update APM to use a SQLite database instead of directly connecting to the Postgres database
More information can be found on this [Trello Card][SQLite]
* Update APM to use a local `.db_connect` file stored in the job directory. More information can be found on this [Trello Card][.db_connect]
* Add more hooks for plugins the needs arise and show the locations where they should be placed.
* Create more plugins as needs for them arise. Identified plugins can be found on this [Trello Card][plugins]
* If data is not available for the process and dates requested the user needs to be able to request the data from Archive. More information can be found on this [Trello Card][request_data]
* Validate db credentials when the user is asked for them. This will avoid the unauthorized error when bad credentials are used.

__Note__: Identified Trello cards have been archived. They can be moved back to a board if the time comes for them to be worked on. The links above are an easy way to reference these cards if they need to be updated or retrieved.

----

## Flowchart
![APM Flowchart](https://raw.githubusercontent.com/ARM-DOE/apm/master/doc_files/APM.png "APM Flowchart")

[apm_mv]: https://trello.com/c/k9ZYhxwG
[SQLite]: https://trello.com/c/Ibsf301D
[.db_connect]: https://trello.com/c/N9P8ub3c
[plugins]: https://trello.com/c/1oTfXxva
[request_data]: https://trello.com/c/PWfnNXkJ
[validate]: https://trello.com/c/IheeQKLm
