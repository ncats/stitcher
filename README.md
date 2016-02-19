InXight Curator
===============

This codebase is based on the latest version of the Play framework
and as such it needs Java 8 to build. Modules are defined under
```modules```. The main Play app is defined in ```app```. To build the
main app, type

```
./activator {target}
```

where ```{target}``` can be one of
{```compile```,```run```,```test```, ```dist```}. Building modules is
similar:

```
./activator {module}/{target}
```

where ```{module}``` is the module name as it appears under ```modules/```
and ```{target}``` can be {```compile```, ```test```}. To run a particular
class in a particular module, use the ```runMain``` syntax, e.g.,

```
./activator "project core" "runMain ix.curation.tools.DuctTape"
```

