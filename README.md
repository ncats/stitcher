Stitcher
========

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
./activator "project stitcher" "runMain ncats.stitcher.tools.DuctTape"
```

Stitching Approach
==================

We propose a graph-based approach to entity stitching and
resolution. Briefly, our approach uses clique detection to do the
stitching and resolution as follows:

1. For a given hypergraph (multi-edge) of stitched entities, extract
connected components based on stitching keys as defined in
```StitchKey```.

2. For each connected component, perform exhaustive clique enumeration
over each stitch key. A clique is a complete subgraph of size 3 or
larger.

3. Next we identify a set of high confidence cliques. A high
confidence clique is a clique for which its members do not belong to
any other clique. All nodes in a clique are merged to become a
stitched node.

4. For the leftover cliques, we perform a sort by descending order of
the value |V| * |E| where |V| and |E| are the clique size and the
cardinality of stitch keys, respectively. Stitched nodes are created
as we iterate through this order ignoring any nodes that have already
been stitched.
