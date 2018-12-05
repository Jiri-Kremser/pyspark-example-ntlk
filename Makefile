IMAGE?=jkremser/openshift-spark:ntlk

.PHONY: install-deps
install-deps:
	-rm -rf deps || true
	pip install -r requirements.txt -t deps
	cd deps
	zip -r ../deps.zip .

.PHONY: build
build: install-deps
	docker build -t $(IMAGE) -f Dockerfile .

.PHONY: run
run:
	spark-submit --py-files deps.zip app.py
