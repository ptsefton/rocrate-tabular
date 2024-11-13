# RO-Crate Tabulator

Python library to turn an RO-Crate into tabular formats.

## Installation

Install [uv](https://docs.astral.sh/uv/), then

    > git clone git@github.com:Sydney-Informatics-Hub/rocrate-tabular.git
    > cd rocrate-tabular
    > uv run src/rocrate_tabular/rocrate_tabular.py -h

`uv run` should create a local venv and install the dependencies

## Usage

To build an SQLite version of a crate:

    > uv run src/rocrate_tabular/rocrate_tabular.py -c path/to/crate -o out.db 

To build a CSV version of the same tables:

    > uv run src/rocrate_tabular/rocrate_tabular.py -c path/to/crate -o out.csv


## Todo

Build this out so it's a better library:

- [ ] unit tests
- [ ] better separation of output formats
- [ ] build tables for kinds of crate entities with sensible defaults 