Initial setup
=============
```bash
git clone --recurse-submodules git@github.com:gtfierro/hoddb.git
cd hoddb
```

Install go
==========
* [Install go](https://golang.org/doc/install)
* source setup.sh:
```bash
. /setup.sh
```

You should now be able to run:
```bash
make proto
make build
make test
make run
```

