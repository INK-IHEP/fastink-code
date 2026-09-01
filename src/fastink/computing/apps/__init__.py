"""FastINK computing job app framework.

Contents:

* ``base.py``     — :class:`JobApp` abstract class + :class:`ConnectResult`
* ``registry.py`` — :func:`register` decorator + module-level registry
                    and aggregators (``iptables_jobtypes``,
                    ``noenv_jobtypes``, ``start_keywords_for``, ...)
* ``shell.sh``    — shared launcher; every job type's run.sh is
                    delegated to from here
* ``_helpers.py`` — small utilities shared by every app's ``connect()``
* ``<name>/``     — one subpackage per built-in job type
                    (vscode, jupyter, vnc, rootbrowse, enode)

Site-specific / experiment-specific apps ship in plugin packages
(e.g. ``ihep_plugin.computing_apps.asic``) and register with the same
:func:`register` decorator at plugin import time; their ``run.sh`` is
located via :meth:`JobApp.run_script_path` from the class's own module.

Add a new (generic, open-source) job type by:

1. Creating ``fastink/computing/apps/<name>/__init__.py`` with a
   :class:`JobApp` subclass decorated with :func:`register`.
2. Dropping the runtime script at ``fastink/computing/apps/<name>/run.sh``.
3. (Optional) Providing ``nginx.location.conf`` next to it if the app
   is served through the reverse proxy.

Nothing else in the codebase needs a hard-coded reference to the new
type -- the registry discovers it automatically.

This module was previously split across ``computing/base.py``,
``computing/registry.py``, ``computing/shell.sh`` and this package
(``computing/apps/``); the framework files were folded in here to
mirror the self-contained layout of ``computing/adapter/`` and
``computing/site/``.
"""
