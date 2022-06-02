# Author: Alessandra Fais
# Date: 17/02/2022

all:
	$(MAKE) -C src

alglib:
	$(MAKE) -C includes/util/alglib

clean:
	$(MAKE) clean -C src

alglib-clean:
	$(MAKE) clean -C includes/util/alglib

.DEFAULT_GOAL := all
.PHONY: all alglib clean alglib-clean