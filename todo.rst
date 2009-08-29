Next release: 0.1.3
===================

* (done) added read from random path to list of fuzz test actions
* fuzz test: add readByMasks() action
* fuzz test: add delete() by mask action
* fuzz test: add remove_conflicts parameter to modify() and insert() actions
* fuzz test: add search() action

Performance tests
-----------------

Need to add some atomic performance tests. They will allow to test different
database implementations using XML RPC (they should somehow take into account
XML RPC latency).

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
