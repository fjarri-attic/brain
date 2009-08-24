Next release: 0.1.2
===================

XML RPC clarification
---------------------

Currently XML RPC in brain is extended comparing to library version:

* keyword arguments support

* ``bytes()`` transparent support (don't need to use Binary explicitly)

* ``tuple`` support

* Non-string keys of dictionaries support

* module-specific exceptions marshalling/unmarshalling

This is performed mainly using hacks and looks ugly. Patch was sent to Python bug tracker
(http://bugs.python.org/issue6701), but it seems to be ignored. So, the way out is to
remove need of XML RPC extensions and perform necessary actions directly in
XML RPC server/client.

XML RPC secure transport
------------------------

Add ability to use HTTPS in ``brain.Client``. This requires `XML RPC clarification`_.

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

Long search conditions
----------------------

Add support for something like (cond, AND, cond, AND, cond).

Improve fuzz tests
------------------

Add more types of requests to fuzz tests - i.e., remove_conflicts in modify() and insert(),
read not only the whole object, but by path and by mask too and so on.
