Next release: 0.1.3
===================

* (done) fixed several bugs with lists autocreation: when new lists are created during modify()
  or insert(), their elements are filled with Nones
* (done) fixed bug in read() logic - it did not raise error when reading from non-existent list element
* (done) added read from random path to list of fuzz test actions
* (done) fuzz test: add readByMasks() action
* (done) fuzz test: add delete() by mask action
* (done) fuzz test: add remove_conflicts parameter to modify() and insert() actions
* (done) make fuzz test return times for each type of request separately
* add non-atomic performance tests: time for functional tests and times for each request in fuzz test
* (done) review functional tests structure and use named test suites from helpers.py
* fuzz test: add search() action

0.1.4
=====

* make rules for field names more strict (so that they could be passed to database as is) -
  probably same as rules for variable names in Python
* refactor modify() and insert() logic in order to reduce calls to database; something like
  prepare and sort list of fields to modify first
* review autovivification rules in modify() and insert() and remove counterintuitive ones

Future
======

Cross-references in text values
-------------------------------

Can't remember why I needed this, but describing it just in case. This feature
will add support for making references to other objects' fields in text values.
This references shoud be updated automatically on their sources update, and resulting
text values should be seend by ``search()``.

Add ability to use lambda as an argument to search()
----------------------------------------------------

This will require some code-walking library.

Test SSL support
----------------

Currently openssl for py3k is not available.
