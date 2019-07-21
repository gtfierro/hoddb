build:
	go build

zip: build
	zip hoddb_test.zip main.go lambda _hod_

doall: build
	rm -rf _hod_
	cd make_backup && go build
	./make_backup/make_backup
	./lambda
