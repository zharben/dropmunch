## dropmunch 
===
* This program processes data file format specifications that are dropped in the "specs" folder relative to the project root.
* Based on available format specifications, data files dropped in the "data" folder relative to the project root are validated, parsed and loaded into postgres

### Installation
* dropmunch was developed in an ubuntu environment.  It probably will run in any environment that has the required python packages, however to be safe you should run it from Ubuntu 14.04 or higher, with the following packages installed :
1. python-3.4
2. python3-dev
3. pip >= 1.5.6
4. virtualbox >= 4.3.10
5. vagrant >= 1.7.4 - e.g. https://dl.bintray.com/mitchellh/vagrant/vagrant_1.7.4_x86_64.deb
* After the required ubuntu and pip packages have been installed, the following steps are required to run dropmunch :
1. Set up the vagrant/postgres box - see vagrant/INSTALL
 * change the postgres credentials from the default values in vagrant/postgres/Vagrant-setup/bootstrap.sh 
2) Run database migrations :
```
# Remove all dropmunch schema changes
$ alembic downgrade base

# Execute all schema changes
$ alembic upgrade head
```
 * These migrations can be run as many times as you wish. However, doing will destroy your precious dropmunch data
3) Run unit tests from the "dropmunch/test" :
 * test/test_munch_data.py
 * test/test_munch_spec.py
4) Run dropmunch:
```
$ bin/munch.sh -h

Options:
    -h --help
    -v      verbose (log level INFO)
    -V      more verbose (log level DEBUG). Warning - outputs DB credentials!
    -c      import_log.num_rows_processed will be updated after EVERY row!
            This allows dropmunch to recover from unexpected crashes to finish
            processing files, however it will slow down processing

