# dumb push pull in s3

get awscli and run aws configure then
`./setup.sh` or dub click `setup.bat` on windows

# sync
- takes the first item in folder_sync_config.txt as the path to the directory to sync.
- takes the second item (after the comma) and assumes a bucket exists in s3 of that name.
- looks at all lines in `extensoin_whitelist.txt` and uploads only files with those extensions.
- `sync.py -u` uploads all files with valid extensions and saves the hashes and times
- `sync.py -d` downloads all files from s3 and saves new hashes and times
- `sync.py -s` syncs files by checking hashes and times to determine what operations should be done.
