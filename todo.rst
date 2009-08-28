Next release: 0.1.3
===================

Performance tests
-----------------

Need to add some atomic performance tests. They will allow to test different
database implementations using XML RPC (they should somehow take into account
XML RPC latency).

Improve fuzz tests
------------------

Add more types of requests to fuzz tests - i.e., remove_conflicts in modify() and insert(),
read not only the whole object, but by path and by mask too and so on.

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
