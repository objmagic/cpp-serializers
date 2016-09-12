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

void
heron_capnproto_serialization_test(size_t iterations)
{
    using namespace capnp_test;

    std::vector<std::string> v({"nathan", "mike", "jackson", "golda", "bertels"});
    size_t word_size = v.size();

    std::chrono::microseconds total_build_time(0);
    std::chrono::microseconds total_serial_time(0);
    std::chrono::microseconds total_deserial_time(0);

    auto start = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < iterations; i++) {

        capnp::MallocMessageBuilder message;

        auto build_start = std::chrono::high_resolution_clock::now();
        // create HeronDataTupleSet builder
        HeronDataTupleSet::Builder hdts_builder = message.getRoot<HeronDataTupleSet>();

        // get StreamId builder and build StreamId
        StreamId::Builder stream_id_builder = hdts_builder.initStream();
        stream_id_builder.setIds("default");
        stream_id_builder.setComponentName("word");

        // each tuple set has 1024 tuples
        size_t SIZE = 1024;

        // get List(HeronDataTuple) builder and build them
        ::capnp::List<HeronDataTuple>::Builder hdt_list_builder = hdts_builder.initTuples(SIZE);
        for (size_t i = 0; i < SIZE; i++) {
            hdt_list_builder[i].setKey(0);
            ::capnp::List<::capnp::Text>::Builder root_ids_list = hdt_list_builder[i].initValues(1);
            root_ids_list.set(0, v[i % word_size]);
        }

        auto build_finish = std::chrono::high_resolution_clock::now();
        total_build_time += std::chrono::duration_cast<std::chrono::microseconds>(build_finish - build_start);

        // serialize
        auto serial_start = std::chrono::high_resolution_clock::now();
        kj::ArrayPtr<const kj::ArrayPtr<const capnp::word>> serialized =
                message.getSegmentsForOutput();
        auto serial_finish = std::chrono::high_resolution_clock::now();

        total_serial_time += std::chrono::duration_cast<std::chrono::microseconds>(serial_finish - serial_start);

        if (i == 0) {
            size_t size = 0;
            for (auto segment: serialized) {
                size += segment.asBytes().size();
            }

            std::cout << "capnproto HeronDataTupleSet size = " << size << " bytes" << std::endl;
        }

        // deserialize
        auto deserial_start = std::chrono::high_resolution_clock::now();
        capnp::SegmentArrayMessageReader reader(serialized);
        auto hdts_from_serial = reader.getRoot<HeronDataTupleSet>();
        auto deserial_finish = std::chrono::high_resolution_clock::now();
        total_deserial_time += std::chrono::duration_cast<std::chrono::microseconds>(deserial_finish - deserial_start);
    }

    auto finish = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(finish - start).count();
    std::cout << "Heron capnproto: time = " << duration << " microseconds ";
    std::cout << "iterations = " << iterations << std::endl << std::endl;
    std::cout << "Total build time = " << total_build_time.count() << " microseconds " << std::endl;
    std::cout << "Total serializing time = " << total_serial_time.count() << " microseconds " << std::endl;
    std::cout << "Total deserializing time = " << total_deserial_time.count() << " microseconds " << std::endl;

    return;

}

void do_something(const flatbuffers_test::HeronDataTupleSet* r){
    return;
}

void
heron_flatbuffers_serialization_test(size_t iterations)
{
    using namespace flatbuffers_test;
    std::vector<std::string> v({"nathan", "mike", "jackson", "golda", "bertels"});

    size_t word_size = v.size();

    // Prepare flatbuffer builder
    flatbuffers::FlatBufferBuilder builder;

    // Generate [HeronDataTuple]
    std::vector<flatbuffers::Offset<HeronDataTuple>> hdts_vector;
    int64_t key = 0;

    // Number of tuples in HeronDataTupleSet
    size_t SIZE = 1024;

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
        for (size_t i = 0; i < SIZE; i++) {
            std::vector<flatbuffers::Offset<flatbuffers::String>> vals;
            vals.push_back(builder.CreateString(v[i % word_size]));
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
        auto heron_data_tuple_set_sample = CreateHeronDataTupleSet(builder, stream_id, hdts_final);
        builder.Finish(heron_data_tuple_set_sample);
        auto build_end = std::chrono::high_resolution_clock::now();
        auto build_duration = std::chrono::duration_cast<std::chrono::microseconds>(build_end - build_start);
        total_build_time += build_duration;
        // finish build

        // prepare deserializing/serializing
        auto serial_start = std::chrono::high_resolution_clock::now();
        auto p = reinterpret_cast<char*>(builder.GetBufferPointer());
        auto sz = builder.GetSize();
        if (i == 0) {
            std::cout << "flatbuffers HeronDataTupleSet size = " << sz << " bytes" << std::endl;
        }
        std::vector<char> buf(p, p + sz);
        auto serial_end = std::chrono::high_resolution_clock::now();
        auto serial_duration = std::chrono::duration_cast<std::chrono::microseconds>(serial_end - serial_start);
        total_serial_time += serial_duration;

        auto deserial_start = std::chrono::high_resolution_clock::now();
        auto r2 = GetHeronDataTupleSet(buf.data());
        auto deserial_end = std::chrono::high_resolution_clock::now();
        auto deserial_duration = std::chrono::duration_cast<std::chrono::microseconds>(deserial_end - deserial_start);
        total_deserial_time += deserial_duration;
        builder.ReleaseBufferPointer();
    }
    // report
    auto finish = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(finish - start).count();
    std::cout << "Total Heron flatbuffer: time = " << duration << " microseconds" << std::endl << std::endl;
    std::cout << "Total build time = " << total_build_time.count() << " microseconds" << std::endl << std::endl;
    std::cout << "Total serialization time = " << total_serial_time.count() << " microseconds" << std::endl << std::endl;
    std::cout << "Total deserialization time = " << total_deserial_time.count() << " microseconds" << std::endl << std::endl;

    return;
}

int
main(int argc, char **argv)
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    if (argc < 2) {
        std::cout << "usage: " << argv[0] << " N [thrift-binary thrift-compact protobuf boost msgpack cereal avro capnproto flatbuffers]";
        std::cout << std::endl << std::endl;
        std::cout << "arguments: " << std::endl;
        std::cout << " N  -- number of iterations" << std::endl << std::endl;
        return EXIT_SUCCESS;
    }

    size_t iterations;

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

    std::cout << "performing " << iterations << " iterations" << std::endl << std::endl;

    /*std::cout << "total size: " << sizeof(kIntegerValue) * kIntegersCount + kStringValue.size() * kStringsCount << std::endl;*/

    try {

        if (names.empty() || names.find("protobuf") != names.end()) {
            protobuf_serialization_test(iterations);
        }

        if (names.empty() || names.find("heron-capnproto") != names.end()) {
            heron_capnproto_serialization_test(iterations);
        }

        if (names.empty() || names.find("heron-flatbuffers") != names.end()) {
            heron_flatbuffers_serialization_test(iterations);
        }
    } catch (std::exception &exc) {
        std::cerr << "Error: " << exc.what() << std::endl;
        return EXIT_FAILURE;
    }

    google::protobuf::ShutdownProtobufLibrary();

    return EXIT_SUCCESS;
}
