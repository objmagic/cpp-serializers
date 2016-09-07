#!/bin/sh

ITERATIONS=16384
FLAT="heron-flatbuffers"
CAPN="heron-capnproto"

# flatbuffers
echo "-O0, flatbuffers"
./benchmark-o0 $ITERATIONS $FLAT

echo "-O1, flatbuffers"
./benchmark-o1 $ITERATIONS $FLAT

echo "-O2, flatbuffers"
./benchmark-o2 $ITERATIONS $FLAT

# flatbuffers tcmalloc
echo "-O0, flatbuffers, with TCMalloc"
./benchmark-o0-tc $ITERATIONS $FLAT

echo "-O1, flatbuffers, with TCMalloc"
./benchmark-o1-tc $ITERATIONS $FLAT

echo "-O2, flatbuffers, with TCMalloc"
./benchmark-o2-tc $ITERATIONS $FLAT

# CapNProto
echo "-O0, CapNProto"
./benchmark-o0 $ITERATIONS $CAPN

echo "-O1, CapNProto"
./benchmark-o1 $ITERATIONS $CAPN

echo "-O2, CapnNProto"
./benchmark-o2 $ITERATIONS $CAPN

# CapNProto tcmalloc
echo "-O0, CapNProto, with TCMalloc"
./benchmark-o0-tc $ITERATIONS $CAPN

echo "-O1, CapNProto, with TCMalloc"
./benchmark-o1-tc $ITERATIONS $CAPN

echo "-O2, CapnNProto, with TCMalloc"
./benchmark-o2-tc $ITERATIONS $CAPN
