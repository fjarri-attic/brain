==========================================
Brain - DDB-like front-end for SQL engines
==========================================

.. contents::

Documentation is under construction
-----------------------------------

Just give me a few days.

Quick start
-----------

This section will show only the simple usage examples. For exhaustive information please consult
the reference entries for corresponding functions.

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

The next function is `modify()`_; it allows us to change the contents of the object.

 >>> conn.modify(id1, ['a'], 2)
 >>> data1 = conn.read(id1)
 >>> print(data1)
 {'a': 2, 'b': 1.345}

Its first argument is object ID, second one is is the `path`_ to some place inside object and
the third one is the value to store (can be either some simple type or data structure). Path
is a list, whose elements can be strings, integers or Nones. String element corresponds to key
in dictionary, integer to list index, and None to list mask.

You may have noticed that the second object contains a list. New elements can be added
to list in two ways - either using `modify()`_ with path, specifying list index to create,
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

The last basic function is `delete()`_. It can delete the whole objects, or its parts
(dictionary keys or list elements).

 >>> print(conn.objectExists(id1))
 True
 >>> conn.delete(id1)
 >>> print(conn.objectExists(id1))
 False
 >>> conn.delete(id2, ['list'])
 >>> print(conn.read(id2))
 {'id1': 1}

Connection should be closed using `close()`_ after it is not longer needed. In case of
in-memory database, of course, all data will be lost after call to `close()`_.

Transaction support
~~~~~~~~~~~~~~~~~~~

One of the main advantages of using the developed SQL engine as a back-end is the
ACID compatibility. As a result, brain front-end has full transaction support too.

If transaction was not started explicitly, the new one is created and committed for
each request (create, modify, insert and so on) implicitly. In case of some underlying
error, this transaction is rolled back, so the request cannot be completed partially.

There are two types of transactions - synchronous and asynchronous. During the
synchronous transaction you get request results instantly; during the asynchronous one
requests do not return any results - all results are returned by `commit()`_ as a list.

Let's illustrate this by several simple examples. First, connect to database and
create some objects.

 >>> import brain
 >>> conn = brain.connect(None, None)
 >>> id1 = conn.create({'a': 1, 'b': 2})
 >>> id2 = conn.create({'c': 3, 'd': 4})

For each of two `create()`_'s above transactions were started and committed implicitly
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
object and passed to DB engine in one single package when `commit()`_ is called. If the user
changes his mind and calls `rollback()`_, all this package is simply discarded.

 >>> conn.beginAsync()
 >>> conn.modify(id1, ['a'], 0)
 >>> conn.read(id1)
 >>> print(conn.commit())
 [None, {'a': 0, 'b': 2}]

In the example above there were two requests inside a transaction; first one, `modify()`_
does not return anything, and the second one, `read()`_, returned object contents.
Therefore `commit()`_ returned both their results as a list.

XML RPC layer
~~~~~~~~~~~~~

Brain has embedded XML RPC server and client. First, we will create and start server:

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
 * Currently the following Python types are supported: None, int, float, str and bytes.
 * Integers are limited to 8 bytes (by DB engines) and to 4 bytes by XML RPC protocol.

Structure limitations:
 * Each object can contain arbitrarily combined values, lists and dictionaries.
 * Structure depth is not limited theoretically, but in practice it is - by DB engine.
 * Lists and dictionaries can be empty.
 * Dictionary keys should have string type.

.. _connect():

brain.connect()
~~~~~~~~~~~~~~~

Connect to the database (or create the new one).

**Arguments**: ``connect(engine_tag, *args, **kwds)``

``engine_tag``:
  String, specifying the DB engine to use. Can be obtained by `getEngineTags()`_.
  If equal to ``None``, the default tag is used; its value can be obtained using `getDefaultEngineTag()`_.

``args``, ``kwds``:
  Engine-specific parameters. See `Engines`_ section for further information.

**Returns**: `Connection`_ object.

.. _Connection:

Connection, RemoteConnection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These objects represent the connection to the database. They have exactly the same public interface,
so only Connection methods will be described.

Currently the following connection methods are available:

 * `begin()`_
 * `beginAsync()`_
 * `beginSync()`_
 * `close()`_
 * `commit()`_
 * `create()`_
 * `delete()`_
 * `deleteMany()`_
 * `dump()`_
 * `insert()`_
 * `insertMany()`_
 * `modify()`_
 * `objectExists()`_
 * `read()`_
 * `readByMask()`_
 * `readByMasks()`_
 * `repair()`_
 * `rollback()`_
 * `search()`_

begin()
=======

Start database transaction. If transaction is already in progress, `FacadeError`_
will be raised.

**Arguments**: ``begin(sync)``

``sync``:
  Boolean value, specifying whether transaction should be synchronous or not
  (see `beginSync()`_ or `beginAsync()`_ correspondingly for details)

beginAsync()
============

This function is an alias for `begin()`_ (equals to ``begin(sync=False)``)

Start asynchronous transaction. During the asynchronous transaction requests to database
are not processed, just stored inside the connection. Correspondingly, actual database
transaction is not started. When `commit()`_ is called, database transaction is created,
and all of requests are being processed at once, and their results are returned from
`commit()`_ as a list.

This decreases the time database is locked by the transaction and increases the speed
of remote operations (one XML RPC multicall is faster than several single calls).
But, of course, this method is less convenient than the synchronous
or implicit transaction.

**Arguments**: ``beginAsync()``

**Example**:

 >>> id1 = conn.create({'name': 'Bob'})
 >>> conn.beginAsync()
 >>> conn.modify(id1, ['name'], 'Carl')
 >>> print(conn.read(id1))
 None
 >>> print(conn.commit())
 [None, {'name': 'Carl'}]

beginSync()
===========

This function is an alias for `begin()`_ (equals to ``begin(sync=True)``)

Start synchronous transaction. During the synchronous transaction request results are available
instantly (for the same connection object), so one can perfomr complex actions inside
one transaction. On the downside, actual database transaction is opened all the time,
probably locking the database (depends on the engine). In case of remote connection,
synchronous transaction means that there will be several requests/responses performed,
slowing down transaction processing.

**Arguments**: ``beginSync()``

**Example**:

 >>> id1 = conn.create({'name': 'Bob'})
 >>> conn.beginSync()
 >>> conn.modify(id1, ['name'], 'Carl')
 >>> print(conn.read(id1))
 {'name': 'Carl'}
 >>> conn.commit()

close()
=======

Close connection to the database. All uncommitted changes will be lost.

**Arguments**: ``close()``

commit()
========

Commit current transaction. If transaction is not in progress, `FacadeError`_ will be raised.

**Arguments**: ``commit()``

create()
========

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

 >>> id1 = conn.create([1, 2, 3])
 >>> print(conn.read(id1))
 [1, 2, 3]

* Creation with path

 >>> id2 = conn.create([1, 2, 3], ['key'])
 >>> print(conn.read(id2))
 {'key': [1, 2, 3]}

.. _delete():

.. _deleteMany():

delete(), deleteMany()
======================

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

 >>> id1 = conn.create({'Tracks': [{'Name': 'track 1', 'Length': 240}, {'Name': 'track 2', 'Length': 300}]})
 >>> conn.delete(id1, ['Tracks', None, 'Length'])
 >>> print(conn.read(id1))
 {'Tracks': [{'Name': 'track 1'}, {'Name': 'track 2'}]}

dump()
======

Get all database contents.

**Arguments**: ``dump()``

**Returns**: dictionary {object ID: object contents}

**Example**:

 >>> id1 = conn.create([1, 2, 3])
 >>> id2 = conn.create({'key': 'val'})
 >>> print(conn.dump())
 {1: [1, 2, 3], 2: {'key': 'val'}}

.. _insert():

.. _insertMany():

insert(), insertMany()
======================

Insert given data to list in object.

**Arguments**:
  ``insert(id, path, value, remove_conflicts=False)``

  ``insertMany(id, path, values, remove_conflicts=False)``

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
  See the description of this parameter for `modify()`_. ``insert()`` tries to perform
  ``modify(id, path, [], remove_conflicts)`` before doing any actions.

**Remarks**:
  * If target object does not have the field, which ``path`` is pointing to, it will be created.

  * If ``path`` points to dictionary key, `FormatError`_ will be raised.

  * If dictionary already exists at the place which ``path`` is pointing to, `StructureError`_
    will be raised.

**Example**:

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

* Autovivification, remove conflicts

 >>> conn.insert(id1, ['key2', 'key3', None], 50, remove_conflicts=True)
 >>> print(conn.read(id1))
 {'key2': {'key3': [50]}, 'key': [0, 1, 2, 3, 4]}

* Insert several values at once

 >>> conn.insertMany(id1, ['key2', None], [51, 52, 53])
 >>> print(conn.read(id1))
 {'key2': {'key3': [50, 51, 52, 53]}, 'key': [0, 1, 2, 3, 4]}

* Insert data structure

 >>> conn.insert(id1, ['key2', 'key3', None], {'subkey': 'val'})
 >>> print(conn.read(id1))
 {'key2': {'key3': [50, 51, 52, 53, {'subkey': 'val'}]}, 'key': [0, 1, 2, 3, 4]}

modify()
========

Modify or create field in object.

**Arguments**: ``modify(id, path, value, remove_conflicts=False)``

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
  modify() is guaranteed to finish successfully and the result of ``read(id, path)`` is
  guaranteed to be equal to ``value``.

  If ``remove_conflicts`` equals ``False``, `StructureError` is raised if conflict is found.

**Example**:

 >>> id1 = conn.create({'key': 'val'})

* Simple modification

 >>> conn.modify(id1, ['key'], 'new_val')
 >>> print(conn.read(id1))
 {'key': 'new_val'}

* Save data structure in place of value

 >>> conn.modify(id1, ['key'], [1, 2])
 >>> print(conn.read(id1))
 {'key': [1, 2]}

* Implicitly transform list remove ``[1, 2]`` using ``remove_conflicts``

 >>> conn.modify(id1, ['key', 'key2'], 'val', remove_conflicts=True)
 >>> print(conn.read(id1))
 {'key': {'key2': 'val'}}

rollback()
==========

Roll current transaction back. If transaction is not in progress, `FacadeError`_ will be raised.

**Arguments**: ``rollback()``

.. _paths:

Path
~~~~

Path to some value in object is a list, which can contain only strings, integers and Nones.
Empty list means the root level of an object; string stands for dictionary key and interger
stands for position in list. None is used in several special cases: to specify that `insert()`_
should perform insertion at the end of the list or as a mask for `delete()`_ and `read()`_.

If path does not contain Nones, it is called *determined*.

**Example**:

 >>> id1 = conn.create({'Tracks': [{'Name': 'track 1', 'Length': 240},
 ... {'Name': 'track 2', 'Length': 300}]})
 >>> print(conn.read(id1, ['Tracks', 0, 'Name']))
 track 1
 >>> print(conn.readByMask(id1, ['Tracks', None, 'Length']))
 {'Tracks': [{'Length': 240}, {'Length': 300}]}

.. _FacadeError:

.. _EngineError:

.. _StructureError:

.. _FormatError:

Exceptions
~~~~~~~~~~

Following exceptions can be thrown by API:

 ``brain.FacadeError``:
   Signals about the error in high-level wrappers. Can be caused by incorrect
   calls to `begin()`_ \\ `commit()`_ \\ `rollback()`_, incorrect engine tag and so on.

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
