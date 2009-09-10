Next release: 0.1.4
===================

* (done) fixed minor bug in read(), when it returned empty list when no fields were found
  which match given masks
* (done) search() uses 2 requests to DB engine instead of (number_of_conditions + 1)
* (done) delete() uses much less requests to DB engine (approximately one request per affected table)
* (done) read() uses less requests to DB engine (number_of_fields + 1)
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
