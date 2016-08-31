@0xead5e4c3b2579756;

$import "/capnp/c++.capnp".namespace("capnp_test");

struct Record {
    ids @0 :List(Int64);
    strings @1 :List(Text);
}

struct StreamId {
    ids @0 :Text;
    componentName @1 :Text;
}

struct RootId {
    taskid @0 :Int32;
    key @1 :Int64;
}

struct HeronDataTuple {
    key @0 :Int64;
    roots @1 :List(RootId);
    values @2 :List(Text);
    deskTaskIds @3 :List(Int32);
}

struct HeronDataTupleSet {
    stream @0: StreamId;
    tuples @1: List(HeronDataTuple);
}