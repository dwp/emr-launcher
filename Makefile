SHELL:=bash

default: help

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: bootstrap
bootstrap: ## Bootstrap local environment for first use
	make git-hooks
	pip install --user pipenv

.PHONY: git-hooks
git-hooks: ## Set up hooks in .git/hooks
	@{ \
		HOOK_DIR=.git/hooks; \
		for hook in $(shell ls .githooks); do \
			if [ ! -h $${HOOK_DIR}/$${hook} -a -x $${HOOK_DIR}/$${hook} ]; then \
				mv $${HOOK_DIR}/$${hook} $${HOOK_DIR}/$${hook}.local; \
				echo "moved existing $${hook} to $${hook}.local"; \
			fi; \
			ln -s -f ../../.githooks/$${hook} $${HOOK_DIR}/$${hook}; \
		done \
	}

.PHONY: clean
clean:
	rm -rf dist
	rm -rf emr-launcher.zip

emr-launcher.zip: clean
	mkdir -p dist
	cp -r emr_launcher dist
	pipenv install && \
	VENV=$$(pipenv --venv) && \
	cp -rf $${VENV}/lib/python3.7/site-packages/* dist/
	cp -rf docs dist/docs
	cd dist && zip -qr ../$@ .

.PHONY: zip
zip: emr-launcher.zip
