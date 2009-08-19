Brain - DDB-like front-end for SQL engines
==========================================

.. contents::

Overview
--------

This is an overview

Quick start
-----------

This section will show only the basic usage examples. For exhaustive information please consult
the reference entries for corresponding functions.

First, import the module:

.. code-block:: python

 >>> import brain

Then we will need to connect to existing database or create the new one. 
In this example we will use the default DB engine (sqlite3) and in-memory database.
First None stands for so called "engine tag" (which identifies DB engine to use),
and the second one stands for DB name (which is mandatory for sqlite3 engine).

.. code-block:: python

 >>> conn = brain.connect(None, None)

Now we can create some objects. Objects are identified by their IDs, which are
intended to be opaque. The only thing that the end user should know is that they
can be stored in database too.

.. code-block:: python

 >>> id1 = conn.create({'a': 1, 'b': 1.345})
 >>> id2 = conn.create({'id1': id1, 'list': [1, 2, 'some_value']})

These objects can be read from database:

.. code-block:: python

 >>> data1 = conn.read(id1)
 >>> print(data1)
 {'a': 1, 'b': 1.345}
 >>> data2 = conn.read(id2)
 >>> print(data2)
 {'list': [1, 2, 'some_value'], 'id1': 1}

You can see that the object ID is, in fact, a simple integer. It is true for sqlite3 engine,
but each engine can use its own ID format.

The next function is modify(); it allows us to change the contents of the object.

.. code-block:: python

 >>> conn.modify(id1, 2, ['a'])
 >>> data1 = conn.read(id1)
 >>> print(data1)
 {'a': 2, 'b': 1.345}

Its first argument is object ID, second one is the value to store (can be either some simple
type or data structure) and the third one is the "path" to some place inside object. Path
is a list, whose elements can be strings, integers or Nones. String element corresponds to key
in dictionary, integer to list index, and None to list mask.

You may have noticed that the second object contains a list. New elements can be added 
to list in two ways - either using modify() with path, specifying list index to create,
or inserting new element to some place in list:

.. code-block:: python

 >>> conn.modify(id2, 3, ['list', 3])
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

.. code-block::python

 >>> import brain.op as op
 >>> objs = conn.search(['list', 0], op.EQ, 4)
 >>> print(objs == [id2])
 True

Search request supports nested conditions and several types of comparisons (including regexps).
See its reference page for more information.

The last basic function is delete(). It can delete the whole objects, or its parts
(dictionary keys or list elements).

.. code-block::python

 >>> print(conn.objectExists(id1))
 True
 >>> conn.delete(id1)
 >>> print(conn.objectExists(id1))
 False
 >>> conn.delete(id2, ['list'])
 >>> print(conn.read(id2))
 {'id1': 1}

Connection should be closed using close() after it is not longer needed. In case of
in-memory database, of course, all data will be lost after call to close().

Transaction support
-------------------

One of the main advantages of using the developed SQL engine as a back-end is the
ACID compatibility. As a result, brain front-end has full transaction support too.

If transaction was not started explicitly, the new one is created and committed for
each request (create, modify, insert and so on) implicitly. In case of some underlying
error, this transaction is rolled back, so the request cannot be completed partially.

There are two types of transactions - synchronous and asynchronous. During the
synchronous transaction you get request results instantly; during asynchronous transaction
requests do not return any results - all results are returned by commit() as a list.

Synchronous transaction
~~~~~~~~~~~~~~~~~~~~~~~




XML RPC layer
-------------

Reference
---------

connect
~~~~~~~

Entry for connect()
