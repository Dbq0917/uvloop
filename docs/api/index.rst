API
===

If you are looking for information on a specific function, class or method,
this part of the documentation is for you.


uvloop
------

.. autoclass:: uvloop.EventLoopPolicy
  :members:

.. autofunction:: uvloop.new_event_loop

.. autoclass:: uvloop.Loop
  :members:
  :undoc-members:
  :inherited-members:


Graceful shutdown
-----------------

.. autoclass:: uvloop.GracefulServer
  :members:

.. autoclass:: uvloop.ShutdownResult
  :members:

.. autoclass:: uvloop.ShutdownState
  :members:

.. autofunction:: uvloop.install_signal_handlers

.. autofunction:: uvloop.serve

