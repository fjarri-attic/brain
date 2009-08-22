Next release: 0.1.0
===================

* (done) make ``path`` mandatory for ``modify()``
* (done) add ``remove_conflicts`` parameter for ``modify()`` and ``insert()``
* (in progress) write proper documentation, save changelog to file in repository
* (in progress) perform code review, rename public functions if necessary

Future
======

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

Policy for conflicts removal
----------------------------

Add ability to set as a parameter to connection the default value of ``remove_conflicts``.
Currently it is set to False so that database conflicts do not pass unnoticed.

Doctests from documentation
---------------------------

Make doctests from all examples in documentation in order to avoid embarassment if
they stop working after some change.
