# DISSBSON
Tool to dissect a bson file into json files for each document, this tool can handle very large bson files when working with a 25GiB
bson file it uses a measly 250MiB of memory.

It also supports the `--pretty` flag to output pretty json files, and `--slice` to output a range of documents.

## Usage
The simplest usage is in the form of:
```sh
$ dissbson <input> -o <output>
```

```sh
$ dissbson --help
```

# License
BSD 3-Clause License
