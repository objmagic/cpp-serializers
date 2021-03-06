#include <string>
#include <set>
#include <iostream>
#include <fstream>
#include <cstdlib>
#include <stdexcept>
#include <memory>
#include <chrono>
#include <sstream>

#include <boost/shared_ptr.hpp>
#include <boost/lexical_cast.hpp>

#include <capnp/message.h>
#include <capnp/serialize.h>

#include "protobuf/test.pb.h"
#include "capnproto/test.capnp.h"
#include "boost/record.hpp"
#include "flatbuffers/test_generated.h"

#include "data.hpp"

size_t iterations;
class Result
{
    public:
        std::string name;
        size_t num_loop;
        size_t tuples_per_set;
        size_t set_size;
        std::chrono::microseconds total_loop_time;
        std::chrono::microseconds total_build_time;
        std::chrono::microseconds total_serializing_time;
        std::chrono::microseconds total_deserializing_time;

        std::string to_string() {
            std::stringstream ss;
            ss << "Test name: " << name << std::endl;
            ss << "Number of loops executed: " << num_loop << std::endl;
            ss << "Tuples per set: " << tuples_per_set << std::endl;
            ss << "Size of set in bytes: " << set_size << std::endl;
            ss << "Total loop time in microseconds: "
              << total_loop_time.count() << std::endl;
            ss << "Total build time in microseconds: "
              << total_build_time.count() << std::endl;
            ss << "Total serializing time in microseconds: "
              << total_serializing_time.count() << std::endl;
            ss << "Total deserializing time in microseconds: "
              << total_deserializing_time.count();
            return ss.str();
        }
};

void
protobuf_serialization_test(size_t iterations)
{
    using namespace protobuf_test;

    Record r1;

    for (size_t i = 0; i < kIntegers.size(); i++) {
        r1.add_ids(kIntegers[i]);
    }

    for (size_t i = 0; i < kStringsCount; i++) {
        r1.add_strings(kStringValue);
    }

    std::string serialized;

    r1.SerializeToString(&serialized);

    // check if we can deserialize back
    Record r2;
    bool ok = r2.ParseFromString(serialized);
    if (!ok /*|| r2 != r1*/) {
        throw std::logic_error("protobuf's case: deserialization failed");
    }

    std::cout << "protobuf: version = " << GOOGLE_PROTOBUF_VERSION << std::endl;
    std::cout << "protobuf: size = " << serialized.size() << " bytes" << std::endl;

    auto start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < iterations; i++) {
        serialized.clear();
        r1.SerializeToString(&serialized);
        r2.ParseFromString(serialized);
    }
    auto finish = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count();

    std::cout << "protobuf: time = " << duration << " milliseconds" << std::endl << std::endl;
}

Result
heron_capnproto_serialization_test()
{
    using namespace capnp_test;

    capnp::MallocMessageBuilder message;

    std::vector<std::string> v({"nathan", "mike", "jackson", "golda", "bertels"});
    size_t word_size = v.size();
    size_t SIZE = 1024;
    size_t tuple_set_size = 0;

    // build, serialize, and deserialize one data tuple set
    // verify serialization/deserialization work, get the data tuple set size
    HeronDataTupleSet::Builder hdts_builder0 = message.getRoot<HeronDataTupleSet>();

    StreamId::Builder stream_id_builder0 = hdts_builder0.initStream();
    stream_id_builder0.setIds("default");
    stream_id_builder0.setComponentName("word");

    ::capnp::List<HeronDataTuple>::Builder hdt_list_builder0 = hdts_builder0.initTuples(SIZE);
    for (size_t i = 0; i < SIZE; i++) {
        hdt_list_builder0[i].setKey(0);
        ::capnp::List<::capnp::Text>::Builder root_ids_list0 = hdt_list_builder0[i].initValues(1);
        root_ids_list0.set(0, v[i % word_size]);
    }

    // serialize
    kj::ArrayPtr<const kj::ArrayPtr<const capnp::word>> serialized0 =
            message.getSegmentsForOutput();
    for (auto segment: serialized0) {
        tuple_set_size += segment.asBytes().size();
    }

    // deserialize
    capnp::SegmentArrayMessageReader reader0(serialized0);
    auto hdts_from_serial0 = reader0.getRoot<HeronDataTupleSet>();

    // make sure serialization/deserialization work fine
    if (hdts_from_serial0.getTuples().size() != SIZE) {
      throw std::logic_error("capnproto's case: deserialization failed");
    }

    // start actual loop benchmarking
    std::chrono::microseconds total_loop_time(0);
    std::chrono::microseconds total_build_time(0);
    std::chrono::microseconds total_serial_time(0);
    std::chrono::microseconds total_deserial_time(0);

    auto start = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < iterations; i++) {

        auto build_start = std::chrono::high_resolution_clock::now();
        // create HeronDataTupleSet builder
        HeronDataTupleSet::Builder hdts_builder = message.getRoot<HeronDataTupleSet>();

        // get StreamId builder and build StreamId
        StreamId::Builder stream_id_builder = hdts_builder.initStream();
        stream_id_builder.setIds("default");
        stream_id_builder.setComponentName("word");

        // each tuple set has 1024 tuples

        // get List(HeronDataTuple) builder and build them
        ::capnp::List<HeronDataTuple>::Builder hdt_list_builder = hdts_builder.initTuples(SIZE);
        for (size_t j = 0; j < SIZE; j++) {
            hdt_list_builder[j].setKey(0);
            ::capnp::List<::capnp::Text>::Builder root_ids_list = hdt_list_builder[j].initValues(1);
            root_ids_list.set(0, v[j % word_size]);
        }

        auto build_finish = std::chrono::high_resolution_clock::now();
        total_build_time +=
          std::chrono::duration_cast<std::chrono::microseconds>(build_finish - build_start);

        // serialize
        auto serial_start = std::chrono::high_resolution_clock::now();
        kj::ArrayPtr<const kj::ArrayPtr<const capnp::word>> serialized =
                message.getSegmentsForOutput();
        auto serial_finish = std::chrono::high_resolution_clock::now();

        total_serial_time +=
          std::chrono::duration_cast<std::chrono::microseconds>(serial_finish - serial_start);

        // deserialize
        auto deserial_start = std::chrono::high_resolution_clock::now();
        capnp::SegmentArrayMessageReader reader(serialized);
        auto hdts_from_serial = reader.getRoot<HeronDataTupleSet>();
        auto deserial_finish = std::chrono::high_resolution_clock::now();
        total_deserial_time +=
          std::chrono::duration_cast<std::chrono::microseconds>(deserial_finish - deserial_start);
    }

    auto finish = std::chrono::high_resolution_clock::now();
    auto total_time = std::chrono::duration_cast<std::chrono::microseconds>(finish - start);

    Result r = Result();
    r.name = "CapNProto";
    r.num_loop = iterations;
    r.tuples_per_set = SIZE;
    r.set_size = tuple_set_size;
    r.total_loop_time = total_time;
    r.total_build_time = total_build_time;
    r.total_serializing_time = total_serial_time;
    r.total_deserializing_time = total_deserial_time;

    return r;
}

Result
heron_flatbuffers_serialization_test()
{
    using namespace flatbuffers_test;
    std::vector<std::string> v({"nathan", "mike", "jackson", "golda", "bertels"});

    size_t word_size = v.size();

    size_t SIZE = 1024;

    // Prepare flatbuffer builder
    flatbuffers::FlatBufferBuilder builder;

    // Generate [HeronDataTuple]
    std::vector<flatbuffers::Offset<HeronDataTuple>> hdts_vector;
    hdts_vector.reserve(SIZE);

    int64_t key = 0;

    size_t tuple_set_size;

    HeronDataTupleBuilder hdt_builder(builder);
    for (size_t i = 0; i < SIZE; i++) {
        std::vector<flatbuffers::Offset<flatbuffers::String>> vals;
        vals.push_back(builder.CreateString(v[i % word_size]));
        hdt_builder.add_key(key);
        hdt_builder.add_values(builder.CreateVector(vals));
        auto heron_data_tuple = hdt_builder.Finish();
        builder.Finish(heron_data_tuple);
        hdts_vector.push_back(heron_data_tuple);
    }

    auto sid = builder.CreateString("default");
    auto component_name = builder.CreateString("word");
    auto stream_id = CreateStreamId(builder, sid, component_name);
    builder.Finish(stream_id);

    auto hdts_final = builder.CreateVector(hdts_vector);
    auto heron_data_tuple_set_sample =
      CreateHeronDataTupleSet(builder, stream_id, hdts_final);
    builder.Finish(heron_data_tuple_set_sample);
    tuple_set_size = builder.GetSize();

    // prepare deserializing/serializing
    auto p = reinterpret_cast<char*>(builder.GetBufferPointer());

    auto r2 = GetHeronDataTupleSet(p);
    if (r2->tuples()->size() != SIZE) {
        throw std::logic_error("flatbuffer's case: deserialization failed");
    }

    std::chrono::microseconds total_loop_time(0);
    std::chrono::microseconds total_build_time(0);
    std::chrono::microseconds total_serial_time(0);
    std::chrono::microseconds total_deserial_time(0);

    auto start = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < iterations; i++) {
        builder.Clear();
        hdts_vector.clear();

        auto build_start = std::chrono::high_resolution_clock::now();
        // start build
        HeronDataTupleBuilder hdt_builder(builder);
        // produce [HeronDataTuple]
        for (size_t j = 0; j < SIZE; j++) {
            std::vector<flatbuffers::Offset<flatbuffers::String>> vals;
            vals.push_back(builder.CreateString(v[j % word_size]));
            hdt_builder.add_key(key);
            hdt_builder.add_values(builder.CreateVector(vals));
            auto heron_data_tuple = hdt_builder.Finish();
            builder.Finish(heron_data_tuple);
            hdts_vector.push_back(heron_data_tuple);
        }
        // produce [Sid]
        auto sid = builder.CreateString("default");
        auto component_name = builder.CreateString("word");
        auto stream_id = CreateStreamId(builder, sid, component_name);
        builder.Finish(stream_id);
        // produce final HeronDataTupleSet
        auto hdts_final = builder.CreateVector(hdts_vector);
        auto heron_data_tuple_set_sample =
          CreateHeronDataTupleSet(builder, stream_id, hdts_final);
        builder.Finish(heron_data_tuple_set_sample);
        auto build_end = std::chrono::high_resolution_clock::now();
        auto build_duration =
          std::chrono::duration_cast<std::chrono::microseconds>(build_end - build_start);
        total_build_time += build_duration;
        // finish build

        // prepare deserializing/serializing
        auto serial_start = std::chrono::high_resolution_clock::now();
        auto p = reinterpret_cast<char*>(builder.GetBufferPointer());
        auto sz = builder.GetSize();
        std::vector<char> buf(p, p + sz);
        auto serial_end = std::chrono::high_resolution_clock::now();
        auto serial_duration =
          std::chrono::duration_cast<std::chrono::microseconds>(serial_end - serial_start);
        total_serial_time += serial_duration;

        auto deserial_start = std::chrono::high_resolution_clock::now();
        auto r2 = GetHeronDataTupleSet(buf.data());
        auto deserial_end = std::chrono::high_resolution_clock::now();
        auto deserial_duration =
          std::chrono::duration_cast<std::chrono::microseconds>(deserial_end - deserial_start);
        total_deserial_time += deserial_duration;
        builder.ReleaseBufferPointer();
    }
    // report
    auto finish = std::chrono::high_resolution_clock::now();
    auto total_time = std::chrono::duration_cast<std::chrono::microseconds>(finish - start);

    Result r = Result();
    r.name = "FlatBuffer";
    r.num_loop = iterations;
    r.tuples_per_set = SIZE;
    r.set_size = tuple_set_size;
    r.total_loop_time = total_time;
    r.total_build_time = total_build_time;
    r.total_serializing_time = total_serial_time;
    r.total_deserializing_time = total_deserial_time;

    return r;
}

int
main(int argc, char **argv)
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    if (argc < 2) {
        std::cout << "usage: " << argv[0] \
          << " N [thrift-binary thrift-compact protobuf boost "
          << "msgpack cereal avro capnproto flatbuffers]";
        std::cout << std::endl << std::endl;
        std::cout << "arguments: " << std::endl;
        std::cout << " N  -- number of iterations" << std::endl << std::endl;
        return EXIT_SUCCESS;
    }


    try {
        iterations = boost::lexical_cast<size_t>(argv[1]);
    } catch (std::exception &exc) {
        std::cerr << "Error: " << exc.what() << std::endl;
        std::cerr << "First positional argument must be an integer." << std::endl;
        return EXIT_FAILURE;
    }

    std::set<std::string> names;

    if (argc > 2) {
        for (int i = 2; i < argc; i++) {
            names.insert(argv[i]);
        }
    }

    try {

        if (names.empty() || names.find("protobuf") != names.end()) {
            protobuf_serialization_test(iterations);
        }

        if (names.empty() || names.find("heron-capnproto") != names.end()) {
          Result r = heron_capnproto_serialization_test();
          std::cout << r.to_string() << std::endl;
        }

        if (names.empty() || names.find("heron-flatbuffers") != names.end()) {
          Result r = heron_flatbuffers_serialization_test();
          std::cout << r.to_string() << std::endl;
        }
    } catch (std::exception &exc) {
        std::cerr << "Error: " << exc.what() << std::endl;
        return EXIT_FAILURE;
    }

    google::protobuf::ShutdownProtobufLibrary();

    return EXIT_SUCCESS;
}
