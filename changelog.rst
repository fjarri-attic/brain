Project history
===============

0.0.1
-----

Initial state. Supports nested hashes and basic requests.

0.0.2
-----

Added nested lists support without insert and delete requests (positions in list
should be specified explicitly).

0.0.3
-----

Added support for insertion and deletion requests in nested lists.

0.0.4
-----

* test_database.py is split, separate module for each request is created
* some unit-tests added
* added request format checks to interface classes
* added unit-tests for interface format checks

0.0.5
-----

* greatly refactored database layer; structure and logic are still kind of tangled though
* fixed some flaws in database logic, added unittests for these cases
* each request is transacted now
* added protection from SQL injection (hid it in db engine-specific class)
* added exceptions to requests

0.0.6
-----

* added support for int, float, binary and null types along with corresponding search conditions.
* logic and structure layers separated almost completely

0.0.7
-----

* added mechanisms for creating/opening the database
* hid ID creation inside database class and made ModifyRequest return ID of the created object
* moved request tests to a higher level, so that they can be used for DBs written using different languages
* created user-friendly API for requests and connections
* added support for processing several requests inside a single transaction
* moved to Python 3.1

0.0.8
-----

* refactored database interface, hid unnecessary details of modules and classes
* refactored database internals, removed redundant request transformations (since they
  are hidden from user now)
* added functionality tests for connection (transaction functionality, explicit/implicit
  transactions, wrong parameters passed to functions)
* added insert_many(), delete_many(), read_many()

0.0.9
-----

* added helper requests (objectExists() and dump())
* added XML RPC server and client (with multicall support for asynchronous transactions)
* added parameters for test run (in-memory/real DB, API/XMLRPC calls and so on)

0.0.10
------

* using value placeholders instead of string concatenations
* added Postgre SQL engine class
* several tests structure improvements

0.0.11
------

* added fuzz test and several functionality tests (including regression tests for found bugs)
* test coverage is 100% for base logic and close to 100% for other code
* added support for empty structures (lists/dicts)

0.0.12
------

* added documenting for XML RPC server
* added db_path parameter to XML RPC server, which will specify the directory where databases
  are created (for databases which use files)
* added function for restoring database integrity (specification/refcounters table and
  listsizes table can be rebuilt using other DB contents)
* added setuptools info and registered in pypi

0.1.0
-----

* ``BrainServer`` and ``BrainClient`` renamed to ``Server`` and ``Client``
* made ``path`` mandatory for ``modify()``
* added ``remove_conflicts`` parameter for ``modify()`` and ``insert()``
* saved changelog and todo list to files in repository
* wrote documentation

0.1.1
-----

* documentation was moved to test module and can be now fed to doctest.testfile()
* fixed several minor typos in documentation
* added examples with raised exceptions to documentation
* using set() when checking search() results in doctests
* fixed bug with non-created hierarchy during autovivification
* added policy for conflicts removal as connection parameter
* added ability to run doctests to test\run.py

0.1.2
-----

* removed Marshaller/Unmarshaller hooks from xmlrpchelpers
* returning list [id, data, id, data, ...] instead of dictionary from dump()
* simplified xmlrpchelpers structure (since custom Marshaller is not needed anymore)
* using lists instead of tuples in search()
* added support for long search conditions
* added note about mandatory keyword parameters dictionary in XML RPC server help.
* not using 'brain.test' in tests (i.e, make brain and brain.test independent)

0.1.3
-----

* fixed fuzz tests work (it was broken after moving FakeConnection to tests in previous release)
* fixed several bugs with lists autocreation: when new lists are created during modify()
  or insert(), their elements are filled with Nones
* fixed bug in read() logic - it did not raise error when reading from non-existent list element
* fuzz test: added tests for all parameters of read() and delete()
* fuzz test: added tests for remove_conflicts parameter in modify() and insert() actions
* made fuzz test return times for each type of request separately
* added non-atomic performance tests: time for functional tests and times for each request in fuzz test
* constructing test suites hierarchically

0.1.4
=====

* fixed minor bug in read(), when it returned empty list when no fields were found
  which match given masks
* search() uses 2 requests to DB engine instead of (number_of_conditions + 1)
* delete() uses much less requests to DB engine (approximately one request per affected table)
* read() uses less requests to DB engine (number_of_fields + 1)
* refactored modify() and insert() logic in order to reduce calls to database:
  now using slightly more than one query per table affected
* conflicts checking logic is slightly more strict now - creating list/map
  in place of value is considered a conflict too
* changed search() behavior a little - now NOT acts more logically

0.1.5
=====

* added CachedConnection class, which noticeably increases speed of reading operations for slow connections
* added getRemoveConflicts() for connection objects
* generalized connection classes structure, moved parts of logic to separate classes
