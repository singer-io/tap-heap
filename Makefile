.DEFAULT_GOAL := test

test:
	pylint tap_heap --disable too-few-public-methods,missing-docstring,protected-access,no-else-return
	pytest ./tests/unittests/
