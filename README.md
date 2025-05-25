This repo contains WIP scripts and infrastructure to compare flags from IQuOD's original validation run of its AutoQC suite, with the reimplementation from NCEI.

## Goals
 - Problem 1: it's not immediately obvious which profiles correspond to which between the two runs; first task is to associate as many of them as possible.
   - Strategy: build the NCEI results into a database we can index and search for each of the AutoQC results to find a best-match.
 - Problem 2: comparing QC flags
   - Strategy: tbd

## Setup & Execution
 - run setup.sh to set up container networking, database and image builds.
 - build NCEI database:
   - place NCEI results in ncei/
   - `cd ncei`
   - `docker container run -it -v $(pwd):/app --network iquod iquod/validation:ncei bash`
   - `cd /app`
   - build a year of the NCEI run into the database, ex `python builddb.py 1991`
 - loop over AutoQC database and try to find matches:
   - place AutoQC results in autoqc/
   - `cd autoqc`
   - `docker container run -it -v $(pwd):/app --network iquod iquod/validation:autoqc bash`
   - `cd /app`
   - `python explore.py`; currently performs a naive search and match to find best NCEI profile for each AutoQC profile, very WIP
