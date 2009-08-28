Next release: 0.1.2
===================

* (done) using lists instead of tuples in search()
* (done) removed hook for tuples in xmlrpchelpers
* (done) returning list [id, data, id, data, ...] instead of dictionary from dump()
* (done) removed hook for non-string dictionary keys in xmlrpchelpers
* (done) removed hook for bytes() in xmlrpchelpers and transform data structures instead
* (done) simplified xmlrpchelpers structure (since custom Marshaller is not needed anymore) and updated
  documentation correspondingly
* (done) not using 'brain.test' in tests (i.e, make brain and brain.test independent)
* (done) added support for long search conditions
* (done) added note about mandatory keyword parameters dictionary in XML RPC server help.

Future
======

Cross-references in text values
-------------------------------

Can't remember why I needed this, but describing it just in case. This feature
will add support for making references to other objects' fields in text values.
This references shoud be updated automatically on their sources update, and resulting
text values should be seend by ``search()``.

Performance tests
-----------------

Need to add some atomic performance tests. They will allow to test different
database implementations using XML RPC (they should somehow take into account
XML RPC latency).

Improve fuzz tests
------------------

Add more types of requests to fuzz tests - i.e., remove_conflicts in modify() and insert(),
read not only the whole object, but by path and by mask too and so on.

Add ability to use lambda as an argument to search()
----------------------------------------------------

This will require some code-walking library.

Test SSL support
----------------

Currently openssl for py3k is not available.

