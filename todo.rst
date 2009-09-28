Next release: 0.1.5
===================

Add ability to use lambda as an argument to search()
----------------------------------------------------

This will require some code-walking library.

Future
======

Cross-references in text values
-------------------------------

Can't remember why I needed this, but describing it just in case. This feature
will add support for making references to other objects' fields in text values.
This references shoud be updated automatically on their sources update, and resulting
text values should be seend by ``search()``.

Test SSL support
----------------

Currently openssl for py3k is not available.
