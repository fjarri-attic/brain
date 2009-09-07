Next release: 0.1.4
===================

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
