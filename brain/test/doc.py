r"""
==========================================
Brain - DDB-like front-end for SQL engines
==========================================

.. contents::

Introduction
------------

Document databases may prove out to be more convenient than relational ones for programs
that operate with a large set of objects with different parameters. With document database
you do not need to develop the database scheme, just toss objects with complex structure
to the database and retreive them by any search criteria. For example, this approach may
be useful for music players (store tracks/albums/artists as objects with cross-references
and references to real files), project management software, bug tracking systems and so on.

This package is, in effect, a wrapper, which makes the relational database engine look
like DDB. Of course, this approach has its drawbacks - noticeable overhead and slow store/retreive
operations. But it gives fast search, transaction support and all features of chosen relational
DB engine (like DB server, secure access, replication and other stuff) for free. Plus, all DB engines
are constantly improving without my attention - isn't it cool?

This package is in beta state now. You can find all planned tasks in todo.rst (not included in
distribution, get it from master branch). I will appreciate any comments and bug reports,
from grammar and spelling errors in documentation to flaws in module architecture.

Quick start
-----------

This section will show only the simple usage examples. For exhaustive information please consult
the `Reference`_ entries for corresponding functions.

Basic functions
~~~~~~~~~~~~~~~

First, import the module:

 >>> import brain

Then we will need to connect to existing database or create the new one.
In this example we will use the default DB engine (sqlite3) and in-memory database.
First None stands for so called "engine tag" (which identifies DB engine to use),
and the second one stands for DB name (which is mandatory for sqlite3 engine).

 >>> conn = brain.connect(None, None)

Now we can create some objects. Objects are identified by their IDs, which are
intended to be opaque. The only thing that the end user should know is that they
can be stored in database too.

 >>> id1 = conn.create({'a': 1, 'b': 1.345})
 >>> id2 = conn.create({'id1': id1, 'list': [1, 2, 'some_value']})

These objects can be read from database:

 >>> data1 = conn.read(id1)
 >>> print(data1)
 {'a': 1, 'b': 1.345}
 >>> data2 = conn.read(id2)
 >>> print(data2)
 {'list': [1, 2, 'some_value'], 'id1': 1}

You can see that the object ID is, in fact, a simple integer. It is true for sqlite3 engine,
but each engine can use its own ID format.

The next function is `Connection.modify()`_; it allows us to change the contents of the object.

 >>> conn.modify(id1, ['a'], 2)
 >>> data1 = conn.read(id1)
 >>> print(data1)
 {'a': 2, 'b': 1.345}

Its first argument is object ID, second one is is the `path`_ to some place inside object and
the third one is the value to store (can be either some simple type or data structure). Path
is a list, whose elements can be strings, integers or Nones. String element corresponds to key
in dictionary, integer to list index, and None to list mask.

You may have noticed that the second object contains a list. New elements can be added
to list in two ways - either using `Connection.modify()`_ with path, specifying list index to create,
or inserting new element to some place in list:

 >>> conn.modify(id2, ['list', 3], 3)
 >>> print(conn.read(id2))
 {'list': [1, 2, 'some_value', 3], 'id1': 1}
 >>> conn.insert(id2, ['list', 0], 4)
 >>> print(conn.read(id2))
 {'list': [4, 1, 2, 'some_value', 3], 'id1': 1}
 >>> conn.insert(id2, ['list', None], 5)
 >>> print(conn.read(id2))
 {'list': [4, 1, 2, 'some_value', 3, 5], 'id1': 1}

First action creates the element with index 3 in list; note that it is expanded automatically.
Second action inserts the new element to the beginning of the list. Third action inserts
the new element to the end of the list.

We can now search for objects in database. For example, we want to find the object, which
has list under 'list' key in dictionary, which, in turn has the first element equal to 4.

 >>> import brain.op as op
 >>> objs = conn.search(['list', 0], op.EQ, 4)
 >>> print(objs == [id2])
 True

Search request supports nested conditions and several types of comparisons (including regexps).
See its reference page for more information.

The last basic function is `Connection.delete()`_. It can delete the whole objects, or its parts
(dictionary keys or list elements).

 >>> print(conn.objectExists(id1))
 True
 >>> conn.delete(id1)
 >>> print(conn.objectExists(id1))
 False
 >>> conn.delete(id2, ['list'])
 >>> print(conn.read(id2))
 {'id1': 1}
 >>> conn.close()

Connection should be closed using `Connection.close()`_ after it is not longer needed. In case of
in-memory database, of course, all data will be lost after call to `Connection.close()`_.

Transaction support
~~~~~~~~~~~~~~~~~~~

One of the main advantages of using the developed SQL engine as a back-end is the
ACID compatibility. As a result, brain front-end has full transaction support too.

If transaction was not started explicitly, the new one is created and committed for
each request (create, modify, insert and so on) implicitly. In case of some underlying
error, this transaction is rolled back, so the request cannot be completed partially.

There are two types of transactions - synchronous and asynchronous. During the
synchronous transaction you get request results instantly; during the asynchronous one
requests do not return any results - all results are returned by `Connection.commit()`_ as a list.

Let's illustrate this by several simple examples. First, connect to database and
create some objects.

 >>> import brain
 >>> conn = brain.connect(None, None)
 >>> id1 = conn.create({'a': 1, 'b': 2})
 >>> id2 = conn.create({'c': 3, 'd': 4})

For each of two `Connection.create()`_'s above transactions were started and committed implicitly
(because there were not any active transactions at the moment). Now we will create synchronous
transaction explicitly:

 >>> conn.beginSync()
 >>> conn.modify(id1, ['a'], 10)
 >>> print(conn.read(id1))
 {'a': 10, 'b': 2}
 >>> conn.commit()
 >>> print(conn.read(id1))
 {'a': 10, 'b': 2}

Note that during synchronous transaction modifications become visible instantly. Now
consider the similar operation inside a transaction, but this time we will roll it back:

 >>> conn.beginSync()
 >>> conn.modify(id1, ['a'], 20)
 >>> print(conn.read(id1))
 {'a': 20, 'b': 2}
 >>> conn.rollback()
 >>> print(conn.read(id1))
 {'a': 10, 'b': 2}

Like in the previous example, modification instantly becomes visible, but after the rollback
it is gone.

Asynchronous transactions are slightly different. During the transaction requests will not
return values, because they are not, in fact, executed - they are stored inside the connection
object and passed to DB engine in one single package when `Connection.commit()`_ is called. If the user
changes his mind and calls `Connection.rollback()`_, all this package is simply discarded.

 >>> conn.beginAsync()
 >>> conn.modify(id1, ['a'], 0)
 >>> conn.read(id1)
 >>> print(conn.commit())
 [None, {'a': 0, 'b': 2}]
 >>> conn.close()

In the example above there were two requests inside a transaction; first one, `Connection.modify()`_
does not return anything, and the second one, `Connection.read()`_, returned object contents.
Therefore `Connection.commit()`_ returned both their results as a list.

XML RPC layer
~~~~~~~~~~~~~

Brain has embedded XML RPC server and client. First, we will create and start server:

 >>> import brain
 >>> srv = brain.Server()
 >>> srv.start()

Now server is active on localhost, port 8000 (by default). It is executed in its own thread,
so `start()`_ returns immediately. If you enter http://localhost:8000 in your browser, you
will get a page with list of functions the server supports.

Then we should create the client - either in this session, in other process or even on
the other computer:

 >>> cl = brain.Client('http://localhost:8000')

And client object gives us the ability to create connections. The format of its ``connect()``
method is the same as for `brain.connect()`_:

 >>> conn = cl.connect(None, None)

This object behaves exactly the same as the `Connection`_ object returned by `brain.connect()`_.
You can try all examples from previous sections - they all should work. In the end you
should close the connection and stop server:

 >>> conn.close()
 >>> srv.stop()

Unlike `start()`_, `stop()`_ waits for server to shut down.

Reference
---------

Known limitations
~~~~~~~~~~~~~~~~~

Value limitations:
 * Currently the following Python types are supported: ``None``, ``int``, ``float``, ``str`` and ``bytes``.
 * Integers are limited to 8 bytes (by DB engines) and to 4 bytes by XML RPC protocol.

Structure limitations:
 * Each object can contain arbitrarily combined values, lists and dictionaries.
 * Structure depth is not limited theoretically, but in practice it is - by DB engine.
 * Lists and dictionaries can be empty.
 * Dictionary keys should have string type.

.. _paths:

Path
~~~~

Path to some value in object is a list, which can contain only strings, integers and Nones.
Empty list means the root level of an object; string stands for dictionary key and integer
stands for position in list. None is used in several special cases: to specify that
`Connection.insert()`_ should perform insertion at the end of the list or as a mask for
`Connection.delete()`_ and `Connection.read()`_.

String elements must not contain uppercase symbols. This is done because each field name
correspond to table name in underlying SQL engine, and some engines ignore case in table names.
So, in order to avoid later search errors, uppercase symbols in field names are simply not allowed -
`FormatError`_ is thrown.

If path does not contain Nones, it is called *determined*.

**Example**:

 >>> conn = brain.connect(None, None)
 >>> id1 = conn.create({'tracks': [{'name': 'track 1', 'length': 240},
 ... {'name': 'track 2', 'length': 300}]})
 >>> print(conn.read(id1, ['tracks', 0, 'name']))
 track 1
 >>> print(conn.readByMask(id1, ['tracks', None, 'length']))
 {'tracks': [{'length': 240}, {'length': 300}]}
 >>> conn.close()

.. _FacadeError:

.. _EngineError:

.. _StructureError:

.. _FormatError:

Exceptions
~~~~~~~~~~

Following exceptions can be thrown by API:

 ``brain.FacadeError``:
   Signals about the error in high-level wrappers. Can be caused by incorrect
   calls to `Connection.begin()`_ \\ `Connection.commit()`_ \\ `Connection.rollback()`_,
   incorrect engine tag and so on.

 ``brain.EngineError``:
   Signals about an error in DB engine wrapper.

 ``brain.StructureError``:
   Signals about error in object/database structure - for example, conflicting fields.

 ``brain.FormatError``:
   Wrong format of supplied data: path is not a list, or have elements of wrong type,
   data has values of wrong type and so on.

Engines
~~~~~~~

Currently two engines are supported:

**sqlite3**:
  SQLite 3 engine, built in Python 3.

  **Arguments**: ``(name, open_existing=None, db_path=None)``

  ``name``:
    Database file name. If equal to ``None``, in-memory database is created.

  ``open_existing``:
    Ignored if ``name`` is equal to None.

    If equal to True, existing database file will be opened or `EngineError`_
    will be raised if it does not exist.

    If equal to False, new database file will be created (in place of the existing one, if
    necessary)

    If equal to None, existing database will be opened or the new one will be created, if
    the database file does not exist.

  ``db_path``:
    If is not None, will be concatenated (using platform-specific path join) with ``name``

**postgre**:
  Postgre 8 engine. Will be used if `py-postgresql <http://python.projects.postgresql.org>`_
  is installed.

  **Arguments**: ``(name, open_existing=None, host='localhost', port=5432, user='postgres',
  password='', connection_limit=-1)``

  ``name``:
    Database name.

  ``open_existing``:
    Same logic as for SQLite3 engine

  ``host``:
    Postgre server name

  ``port``:
    Postgre server port

  ``user``, ``password``:
    Credentials for connecting to Postgre server

  ``connection_limit``:
    Connection limit for newly created database. Unlimited by default.

Tests
~~~~~

The package is supplied with a set of functionality tests which I use for debugging purposes.
They can be found in ``test`` subfolder of module main folder. Tests are executed using ``run.py``,
which has the following parameters:

``run.py <func|fuzz|doc|perf> [options]``

``func``:
  Functionality tests. They are based on Python's ``unittest`` module, with some minor extensions.
  Currently they provide almost 100% coverage of package code.

``fuzz``:
  Several objects with random data are created and random actions (`Connection.modify()`_,
  `Connection.insert()`_, `Connection.read()`_, `Connection.delete()`_) are performed on them.
  After each action result is compared to   the result of ``FakeConnection``, which uses Python
  data structures to emulate package behavior.

``doc``:
  Test examples in this documentation using Python's ``doctest`` module.

``perf``:
  Simple non-atomic performance tests (measuring times of ``func`` tests plus combining
  results of ``fuzz`` tests with several predefined seeds).

**global parameters**:
  ``-v LEVEL``, ``--verbosity=LEVEL``:
    Integer from 0 (less verbose) to 3 (more verbose), specifying the amount of information
    which is displayed during tests. Default is 2.

    For ``doc`` tests verbosity level 3 and above enables ``verbose=True`` mode (which means
    that all tests will be shown); otherwise only errors will be shown.

``func`` **parameters**:
  ``--ae``, ``--all-engines``:
    If specified, all available DB engines will be tested. If not specified, only the default
    engine (see `getDefaultEngineTag()`_) will be tested.

  ``--ac``, ``--all-connections``:
    If specified, all available connections will be tested (local, XML RPC and so on). If
    not specified, only local connection will be tested.

  ``--as``, ``--all-storages``:
    If specified, all storage types for each engine will be tested (for example, for sqlite3
    available types are in-memory and file). If not specified, only the default storage for
    each engine will be tested.

``fuzz`` **parameters**:
  ``-o NUM``, ``--objects=NUM``:
    Number of object to be tested simultaneously. Default is 1.

  ``-a NUM``, ``--actions=NUM``
    Number of actions to be performed for one object. Default is 100.

  ``-s SEED``, ``--seed=SEED``
    Integer which will be used as starting seed for random number generator. This wil allow
    to get reproduceable results. By default, random seed is generated.

.. _connect():

brain.connect()
~~~~~~~~~~~~~~~

Connect to the database (or create the new one).

**Arguments**: ``connect(engine_tag, *args, remove_conflicts=False, **kwds)``

``engine_tag``:
  String, specifying the DB engine to use. Can be obtained by `getEngineTags()`_.
  If equal to ``None``, the default tag is used; its value can be obtained using `getDefaultEngineTag()`_.

``remove_conflicts``:
  Default value of this parameter for `Connection.modify()`_ and `Connection.insert()`_.

``args``, ``kwds``:
  Engine-specific parameters. See `Engines`_ section for further information.

**Returns**: `Connection`_ object.

.. _getDefaultEngineTag():

brain.getDefaultEngineTag()
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Get engine tag, which will be used if ``None`` is specified as engine tag in `connect()`_.

**Arguments**: ``getDefaultEngineTag()``

**Returns**: default engine tag.

.. _getEngineTags():

brain.getEngineTags()
~~~~~~~~~~~~~~~~~~~~~

Get available engine tags.

**Arguments**: ``getEngineTags()``

**Returns**: list of engine tags.

.. _operators:

.. _op:

brain.op
~~~~~~~~

This submodule contains operator definitions for `Connection.search()`_ request:

* inversion operator ``NOT`` - can be used in all conditions.

* logical operators ``OR`` and ``AND`` - can be used to link simple conditions.

* comparison operators ``EQ`` (equal to), ``REGEXP``, ``LT`` (lower than), ``LTE`` (lower than or equal to),
  ``GT`` (greater than) and ``GTE`` (greater than or equal to) - can be used in simple conditions.

  * ``EQ`` can be used for all value types.

  * ``REGEXP`` can be used only for strings. It should support POSIX regexps.

  * ``LT``, ``LTE``, ``GT`` and ``GTE`` can be used for integers and floats.

.. _Connection:

.. _RemoteConnection:

Connection, RemoteConnection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These objects represent the connection to the database. They have exactly the same public interface,
so only Connection methods will be described.

Currently the following connection methods are available:

 * `Connection.begin()`_
 * `Connection.beginAsync()`_
 * `Connection.beginSync()`_
 * `Connection.close()`_
 * `Connection.commit()`_
 * `Connection.create()`_
 * `Connection.delete()`_
 * `Connection.deleteMany()`_
 * `Connection.dump()`_
 * `Connection.getRemoveConflicts()`_
 * `Connection.insert()`_
 * `Connection.insertMany()`_
 * `Connection.modify()`_
 * `Connection.objectExists()`_
 * `Connection.read()`_
 * `Connection.readByMask()`_
 * `Connection.readByMasks()`_
 * `Connection.repair()`_
 * `Connection.rollback()`_
 * `Connection.search()`_

Connection.begin()
==================

Start database transaction. If transaction is already in progress, `FacadeError`_
will be raised.

**Arguments**: ``begin(sync)``

``sync``:
  Boolean value, specifying whether transaction should be synchronous or not
  (see `Connection.beginSync()`_ or `Connection.beginAsync()`_ correspondingly for details)

**Example**:

* Start new transaction

 >>> conn = brain.connect(None, None)
 >>> conn.begin(sync=True)

* Failed attempt to start transaction when another one is in progress

 >>> conn.begin(sync=True)
 Traceback (most recent call last):
 ...
 brain.interface.FacadeError: Transaction is already in progress
 >>> conn.close()

Connection.beginAsync()
=======================

This function is an alias for `Connection.begin()`_ (equals to ``begin(sync=False)``)

Start asynchronous transaction. During the asynchronous transaction requests to database
are not processed, just stored inside the connection. Correspondingly, actual database
transaction is not started. When `Connection.commit()`_ is called, database transaction is created,
and all of requests are being processed at once, and their results are returned from
`Connection.commit()`_ as a list.

This decreases the time database is locked by the transaction and increases the speed
of remote operations (one XML RPC multicall is faster than several single calls).
But, of course, this method is less convenient than the synchronous
or implicit transaction.

**Arguments**: ``beginAsync()``

**Example**:

 >>> conn = brain.connect(None, None)
 >>> id1 = conn.create({'name': 'Bob'})
 >>> conn.beginAsync()
 >>> conn.modify(id1, ['name'], 'Carl')
 >>> print(conn.read(id1))
 None
 >>> print(conn.commit())
 [None, {'name': 'Carl'}]
 >>> conn.close()

Connection.beginSync()
======================

This function is an alias for `Connection.begin()`_ (equals to ``begin(sync=True)``)

Start synchronous transaction. During the synchronous transaction request results are available
instantly (for the same connection object), so one can perform complex actions inside
one transaction. On the downside, actual database transaction is opened all the time,
probably locking the database (depends on the engine). In case of remote connection,
synchronous transaction means that there will be several requests/responses performed,
slowing down transaction processing.

**Arguments**: ``beginSync()``

**Example**:

 >>> conn = brain.connect(None, None)
 >>> id1 = conn.create({'name': 'Bob'})
 >>> conn.beginSync()
 >>> conn.modify(id1, ['name'], 'Carl')
 >>> print(conn.read(id1))
 {'name': 'Carl'}
 >>> conn.commit()
 >>> conn.close()

Connection.close()
==================

Close connection to the database. All uncommitted changes will be lost.

**Arguments**: ``close()``

Connection.commit()
===================

Commit current transaction. If transaction is not in progress, `FacadeError`_ will be raised.

**Arguments**: ``commit()``

**Example**:

* Create and commit transaction

 >>> conn = brain.connect(None, None)
 >>> conn.beginSync()
 >>> conn.commit()

* Try to commit non-existent transaction

 >>> conn.commit()
 Traceback (most recent call last):
 ...
 brain.interface.FacadeError: Transaction is not in progress
 >>> conn.close()

Connection.create()
===================

Create new object in database.

**Arguments**: ``create(self, data, path=None)``

``data``:
  Initial object contents. Can be either a value of allowed type, list or dictionary.

``path``:
  If defined, specifies the `path`_ where ``data`` will be stored (if equal to ``None``,
  data is stored in root). Should be determined.

**Returns**: object ID

**Example**:

* Creation without path

 >>> conn = brain.connect(None, None)
 >>> id1 = conn.create([1, 2, 3])
 >>> print(conn.read(id1))
 [1, 2, 3]

* Creation with path

 >>> id2 = conn.create([1, 2, 3], ['key'])
 >>> print(conn.read(id2))
 {'key': [1, 2, 3]}
 >>> conn.close()

.. _Connection.delete():

.. _Connection.deleteMany():

Connection.delete(), Connection.deleteMany()
============================================

Delete the whole object or some of its fields. If an element of list is deleted,
other list elements are shifted correspondingly.

**Arguments**:
  ``delete(id, path=None)``

  ``deleteMany(id, paths=None)``

**Note**: ``delete(id, path)`` is an alias for ``deleteMany(id, [path])``

``id``:
  Target object ID.

``paths``:
  List of `paths`_. If given, is used as the set of masks, specifying fields to delete.
  If ``None``, the whole object will be deleted.

**Example**:

* Deletion of the whole object

 >>> conn = brain.connect(None, None)
 >>> id1 = conn.create([1, 2, 3])
 >>> conn.delete(id1)
 >>> print(conn.objectExists(id1))
 False

* Deletion of specific field

 >>> id1 = conn.create([1, 2, 3])
 >>> conn.delete(id1, [1])
 >>> print(conn.read(id1))
 [1, 3]

* Deletion by mask

 >>> id1 = conn.create({'tracks': [{'name': 'track 1', 'length': 240},
 ... {'name': 'track 2', 'length': 300}]})
 >>> conn.delete(id1, ['tracks', None, 'length'])
 >>> print(conn.read(id1))
 {'tracks': [{'name': 'track 1'}, {'name': 'track 2'}]}
 >>> conn.close()

Connection.dump()
=================

Get all database contents.

**Arguments**: ``dump()``

**Returns**: list [object 1 ID, object 1 contents, object 2 ID, object 2 contents, ...]

**Example**:

 >>> conn = brain.connect(None, None)
 >>> id1 = conn.create([1, 2, 3])
 >>> id2 = conn.create({'key': 'val'})
 >>> print(conn.dump())
 [1, [1, 2, 3], 2, {'key': 'val'}]
 >>> conn.close()

Connection.getRemoveConflicts()
===============================

Get current default value of ``remove_conflicts`` keyword (the one which was set
when connection was created, in `connect()`_).

**Arguments**: ``getRemoveConflicts()``

**Returns**: True or False

.. _Connection.insert():

.. _Connection.insertMany():

Connection.insert(), Connection.insertMany()
============================================

Insert given data to list in object.

**Arguments**:
  ``insert(id, path, value, remove_conflicts=None)``

  ``insertMany(id, path, values, remove_conflicts=None)``

**Note**: ``insert(id, path, value, remove_conflicts)`` is an alias for
``insert(id, path, [value], remove_conflicts)``

``id``:
  Target object ID.

``path``:
  `Path`_ to insert to. Should point to list element (i.e., end with integer or ``None``) and
  be determined (except for, probably, the last element). If the last element is ``None``,
  insertion will be performed to the end of the list.

``value``:
  Data to insert - should be a supported data structure.

``remove_conflicts``
  See the description of this parameter for `Connection.modify()`_. ``insert()`` tries to perform
  ``modify(id, path, [], remove_conflicts)`` before doing any actions.

**Remarks**:
  * If target object does not have the field, which ``path`` is pointing to, it will be created.

  * If ``path`` points to dictionary key, `FormatError`_ will be raised.

  * If dictionary already exists at the place which ``path`` is pointing to, `StructureError`_
    will be raised.

**Example**:

 >>> conn = brain.connect(None, None)
 >>> id1 = conn.create({'key': [1, 2, 3]})

* Insertion to the beginning

 >>> conn.insert(id1, ['key', 0], 0)
 >>> print(conn.read(id1))
 {'key': [0, 1, 2, 3]}

* Insertion to the end

 >>> conn.insert(id1, ['key', None], 4)
 >>> print(conn.read(id1))
 {'key': [0, 1, 2, 3, 4]}

* Autovivification, no conflicts

 >>> conn.insert(id1, ['key2', None], 50)
 >>> print(conn.read(id1))
 {'key2': [50], 'key': [0, 1, 2, 3, 4]}

* Autovivification raises error on existing conflicts

 >>> conn.insert(id1, ['key2', 'key3', None], 50)
 Traceback (most recent call last):
 ...
 brain.interface.StructureError: Path ['key2', 'key3'] conflicts with existing structure

* Autovivification, remove conflicts

 >>> conn.insert(id1, ['key2', 'key3', None], 50, remove_conflicts=True)
 >>> print(conn.read(id1))
 {'key2': {'key3': [50]}, 'key': [0, 1, 2, 3, 4]}

* Insert several values at once

 >>> conn.insertMany(id1, ['key2', 'key3', None], [51, 52, 53])
 >>> print(conn.read(id1))
 {'key2': {'key3': [50, 51, 52, 53]}, 'key': [0, 1, 2, 3, 4]}

* Insert data structure

 >>> conn.insert(id1, ['key2', 'key3', None], {'subkey': 'val'})
 >>> print(conn.read(id1))
 {'key2': {'key3': [50, 51, 52, 53, {'subkey': 'val'}]}, 'key': [0, 1, 2, 3, 4]}

* Try to pass wrong path to insert()

 >>> conn.insert(id1, ['key2', 'key3'], 'val')
 Traceback (most recent call last):
 ...
 brain.interface.FormatError: Last element of target field name should be None or integer
 >>> conn.close()

Connection.modify()
===================

Modify or create field in object.

**Arguments**: ``modify(id, path, value, remove_conflicts=None)``

``id``:
  Target object ID.

``path``:
  Path where to store data.

``value``:
  Data to save at target path.

``remove_conflicts``:
  Determines the way conflicts of ``path`` with existing data structure are handled. Possible conflicts are:

  * ``path`` points to dictionary, when list already exists on the same level

  * ``path`` points to list, when dictionary already exists on the same level

  * ``path`` points to list or dictionary, when scalar value already exists on the same level

  If ``remove_conflicts`` equals ``True``, all conflicting fields are deleted. In other words,
  ``modify()`` is guaranteed to finish successfully and the result of ``read(id, path)`` is
  guaranteed to be equal to ``value``.

  If ``remove_conflicts`` equals ``False``, `StructureError` is raised if conflict is found.

  If ``remove_conflicts`` equals None (default), the value given to `brain.connect()`_ is used.

**Example**:

 >>> conn = brain.connect(None, None)
 >>> id1 = conn.create({'key': 'val'})

* Simple modification

 >>> conn.modify(id1, ['key'], 'new_val')
 >>> print(conn.read(id1))
 {'key': 'new_val'}

* Save data structure in place of value

 >>> conn.modify(id1, ['key'], [1, 2])
 >>> print(conn.read(id1))
 {'key': [1, 2]}

* Try to autovivify conflicting path without ``remove_conflicts`` set

 >>> conn.modify(id1, ['key', 'key2'], 'val')
 Traceback (most recent call last):
 ...
 brain.interface.StructureError: Path ['key', 'key2'] conflicts with existing structure

* Implicitly transform list remove ``[1, 2]`` using ``remove_conflicts``

 >>> conn.modify(id1, ['key', 'key2'], 'val', remove_conflicts=True)
 >>> print(conn.read(id1))
 {'key': {'key2': 'val'}}
 >>> conn.close()

Connection.objectExists()
=========================

Check if object with given ID exists.

**Arguments**: ``objectExists(id)``

``id``:
  Object ID.

**Returns**: True if object with given ID exists, False otherwise.

.. _Connection.read():

.. _Connection.readByMask():

.. _Connection.readByMasks():

Connection.read(), Connection.readByMask, Connection.readByMasks()
==================================================================

Read contents of given object.

**Arguments**:
  ``read(id, path=None, masks=None)``

  ``readByMask(id, mask=None)``

  ``readByMasks(id, masks=None)``

**Note**: ``readByMask(id, mask)`` is an alias for ``readByMasks(id, [mask])`` and
``readByMasks(id, masks)``, in turn, is an alias for ``read(id, None, masks)``.

``id``:
  Target object ID.

``path``:
  `Path`_ to read from. Read from root by default.

``masks``:
  List of `paths`_; all results which do not have one of them in the beginning, will be filtered out.
  Masks are relative to ``path``.

**Returns**: resulting data structure.

**Example**:

 >>> conn = brain.connect(None, None)
 >>> id1 = conn.create({'tracks': [{'name': 'track 1', 'length': 240}, {'name': 'track 2', 'length': 300}]})

* Read the whole object

 >>> print(conn.read(id1))
 {'tracks': [{'length': 240, 'name': 'track 1'}, {'length': 300, 'name': 'track 2'}]}

* Read from given path

 >>> print(conn.read(id1, ['tracks', 0]))
 {'length': 240, 'name': 'track 1'}

* Read by mask

 >>> print(conn.readByMask(id1, ['tracks', None, 'length']))
 {'tracks': [{'length': 240}, {'length': 300}]}

* Read from path, filter by mask. Note that mask is relative.

 >>> print(conn.read(id1, ['tracks'], [[None, 'length']]))
 [{'length': 240}, {'length': 300}]
 >>> conn.close()

Connection.repair()
===================

Internal database structure includes some redundant tables, which are used to increase
database performance. This function can restore them based on actual field data stored in
database. It can be used when database requests (even `Connection.read()`_) are returning strange
errors with long call stack. These internal tables can be spoiled either by errors in logic
or because of some errors in underlying SQL engine.

**Arguments**: ``repair()``

Connection.rollback()
=====================

Roll current transaction back. If transaction is not in progress, `FacadeError`_ will be raised.

**Arguments**: ``rollback()``

**Example**:

* Create and rollback transaction

 >>> conn = brain.connect(None, None)
 >>> conn.beginSync()
 >>> conn.rollback()

* Try to rollback non-existent transaction

 >>> conn.rollback()
 Traceback (most recent call last):
 ...
 brain.interface.FacadeError: Transaction is not in progress
 >>> conn.close()

Connection.search()
===================

Search for objects in database which satisfy given conditions.

**Arguments**: ``search(condition)``

``condition``:
  One of three possibilities:

  * List ``condition``

  * Tuple ``condition``

  * Empty list (only at root level, cannot be a part of condition)

  Simple ``condition`` is a list [``brain.op.NOT``, ] `path`_, comparison_operator, value; complex
  ``condition`` is a list [``brain.op.NOT``, ] ``condition``, [[logical_operator,
  [``brain.op.NOT``, ] ``condition``, ] ... ], where each ``condition`` can be either simple or complex.

  On the root level, you may not wrap condition in a list, but rather just pass
  it as a tuple of arguments to function.

  Logical_operator and comparison_operator - any `operators`_. Value should be a
  scalar of supported type. Note that different values support different type of
  comparisons; see `brain.op`_ reference for details.

  If ``condition`` is an empty list, it matches all existing object IDs in database.

  If condition uses path, not existing in some object, condition is considered
  to be false for this object if it does not contain ``brain.op.NOT`` and true
  otherwise.

  Compound conditions are evaluated successively: ``[cond1, op1, cond2, op2, cond3]`` is evaluated
  as ``[[cond1, op1, cond2], op2, cond3]``.

  In compound conditions ``NOT`` applies to the condition next to it:
  ``[NOT, cond1, op1, cond2, op2, NOT, cond3]`` is evaluated as
  ``[[[NOT cond1], op1, cond2], op2, [NOT, cond3]]``.

**Returns**: list of object IDs, satisfying given conditions (note that order can
depend on DB engine).

**Example**:

 >>> import brain.op as op
 >>> conn = brain.connect(None, None)
 >>> id1 = conn.create({'name': 'Alex', 'age': 22})
 >>> id2 = conn.create({'name': 'Bob', 'height': 180, 'age': 25})
 >>> id3 = conn.create({'name': 'Carl', 'height': 170, 'age': 26})

* Empty condition

 >>> print(set(conn.search()) == set([id1, id2, id3]))
 True

* Simple condition

 >>> print(conn.search(['name'], op.EQ, 'Alex') == [id1])
 True

* Compound condition

 >>> print(set(conn.search([['name'], op.EQ, 'Alex'], op.OR,
 ... [['name'], op.EQ, 'Carl'])) == set([id1, id3]))
 True

* Compound condition with negative

 >>> print(set(conn.search([['name'], op.EQ, 'Alex'], op.OR,
 ... [op.NOT, ['name'], op.EQ, 'Carl'])) == set([id1, id2]))
 True

* Condition with non-equality

 >>> print(conn.search(['age'], op.GT, 25) == [id3])
 True

* Condition with non-existent field

 >>> print(conn.search([['name'], op.EQ, 'Alex'], op.AND,
 ... [['height'], op.EQ, 180]) == [])
 True

* Condition with non-existent field and negative

 >>> print(conn.search([['name'], op.EQ, 'Alex'], op.AND,
 ... [op.NOT, ['height'], op.EQ, 180]) == [id1])
 True

* Long compound condition

 >>> print(conn.search([['name'], op.EQ, 'Alex'], op.OR,
 ... [['age'], op.EQ, 25], op.AND,
 ... [['height'], op.GT, 175]) == [id2])
 True
 >>> conn.close()

CachedConnection
~~~~~~~~~~~~~~~~

This class wraps anything with `Connection`_-like interface and adds object caching.
The caching algorithm is rather simple, it speeds up only read operations (by keeping
copies of objects in memory).

**Warning**: This class can work incorrectly if more than one connection to database is opened.
For example, if the second connection changes something in database, the cache will not change
and, therefore, read operation from the first connection will return the old value.

**Arguments**: ``CachedConnection(conn, size_threshold=0)``

``conn``:
  Object with `Connection`_ interface.

``size_threshold``:
  How many objects the cache must keep in memory. If zero, all accessed objects are kept.
  If non-zero, specifies the number of most recently accessed object kept.

Client
~~~~~~

XML RPC client for brain DB. Based on Python's built-in ``xmlrpc.client.ServerProxy`` and has the
following extensions:

* Supports keyword arguments to calls (adds dictionary with keyword argument to each method call)

* Unmarshalls known `exceptions`_ from ``Faults`` returned by server

**Arguments**: ``Client(addr)``

``addr``:
  Address to connect to.

Client.connect()
================

Connect to DB or create a new one.

**Arguments**: same as for `brain.connect()`_.

**Returns**: `RemoteConnection`_ object.

Client.getDefaultEngineTag()
============================

Same as `brain.getDefaultEngineTag()`_.

Client.getEngineTags()
======================

Same as `brain.getEngineTags()`_.

Server
~~~~~~

XML RPC server for database. Based on Python's built-in ``xmlrpc.server.DocXMLRPCServer``
and extends standard XML RPC slightly: it supports keyword arguments to calls.
They are passed as the dictionary in additional argument to each function.
If function does not have any keyword arguments, empty dictionary is passed.

**Arguments**: ``Server(port=8000, name=None, db_path=None)``

``port``:
  Port where server will wait for requests.

``name``:
  Server thread name.

``db_path``:
  Will be used with DB engines, which store information in files - ``db_path`` will serve as
  a prefix to each created DB file.

.. _start():

Server.start()
==============

Start server in a separate thread. Returns instantly.

**Arguments**: ``start()``

.. _stop():

Server.stop()
=============

Shutdown server and wait for its thread to stop.

**Arguments**: ``stop()``
"""

import doctest
import time
import sys

DOCUMENTATION = __doc__

def runDocTest(verbosity):
	print("Running doctest")

	print("=" * 70)
	time1 = time.time()
	doctest.testmod(m=sys.modules.get(__name__), verbose=False if verbosity < 3 else True)
	time2 = time.time()
	print("=" * 70)

	print("Finished in {0:.3f} seconds".format(time2 - time1))
