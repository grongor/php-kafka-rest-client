### BEGIN main targets

.PHONY: build
build: vendor

.PHONY: list
list:
	@$(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print $$1}}' | sort | egrep -v -e '^[^[:alnum:]]' -e '^$@$$'

### END

### BEGIN secondary targets

.PHONY: vendor
vendor: vendor/lock

vendor/lock: composer.json
	$(MAKE) validate-composer
	$(MAKE) update
	touch vendor/lock

.PHONY: update
update:
	composer update

### END

### BEGIN tests

.PHONY: test
test:
	vendor/bin/phpunit $(PHPUNIT_ARGS)

.PHONY: lint
lint:
	vendor/bin/parallel-lint src/ tests/

.PHONY: cs
cs: vendor
	vendor/bin/phpcs $(PHPCS_ARGS)

.PHONY: cs-fix
cs-fix: vendor
	vendor/bin/phpcbf

.PHONY: phpstan
phpstan: vendor
	vendor/bin/phpstan analyse

.PHONY: validate-composer
validate-composer:
	composer validate --strict

.PHONY: check
check: build validate-composer lint cs phpstan test

### END

### BEGIN cleaning

.PHONY: clean
clean: clean-cache clean-vendor

.PHONY: clean-cache
clean-cache:
	rm -rf var/cache/*

.PHONY: clean-vendor
clean-vendor:
	rm -rf vendor

### END
