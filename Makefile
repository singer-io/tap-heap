.DEFAULT_GOAL := test

check_prereqs:
	bash -c '[[ -n $$VIRTUAL_ENV ]]'
	bash -c '[[ $$(python3 --version) == *3.5.2* ]]'
	bash -c '[[ $$(type -t apt) == file ]]'
	bash -c 'apt -qq list --installed libsnappy-dev 2>/dev/null| grep -Fq -- installed'

test: check_prereqs
	pylint tap_heap --disable too-few-public-methods,missing-docstring,protected-access,no-else-return
	python -m unittest discover
