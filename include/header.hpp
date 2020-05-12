// Copyright 2018 Your Name <your_email>

#ifndef INCLUDE_HEADER_HPP_
#define INCLUDE_HEADER_HPP_
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <iostream>
#include <string>
#include "picosha2.h"
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <boost/thread/thread.hpp>
#include <queue>
#include <vector>
#include <utility>
#include <map>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/sources/severity_feature.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/expressions.hpp>

namespace logging = boost::log;

namespace keywords = boost::log::keywords;

using namespace rocksdb;

class FillDataBase {
public:
    FillDataBase(std::string dbPath =
    "/Users/dellvin/Desktop/lab10/row_dellvin_db")
            : dbPath(dbPath) {}

    void fill(uint64_t familiesCount = 10, uint64_t fieldsCount = 100) {
        createDB(familiesCount);
        fillDB(familiesCount, fieldsCount);
    }

private:
    void createDB(uint64_t familiesCount) {
        Options options;
        options.create_if_missing = true;
        Status s = DB::Open(options, dbPath, &db);
        assert(s.ok());
        ColumnFamilyHandle *cf;
        for (uint64_t i = 0; i < familiesCount; ++i) {
            // create column family
            std::string familyNum = "family_";
            familyNum += std::to_string(i);
            s = db->CreateColumnFamily(ColumnFamilyOptions(), familyNum, &cf);
            assert(s.ok());
        }
        // close DB
        s = db->DestroyColumnFamilyHandle(cf);
        assert(s.ok());
        delete db;
    }

    void fillDB(uint64_t familiesCount, uint64_t fieldsCount) {
        // open DB with two column families
        std::vector <std::string> column_families;
        DB::ListColumnFamilies(DBOptions(),
                               dbPath, &column_families);
        std::vector <ColumnFamilyDescriptor> column_fam;
        // open the new one, too
        for (const auto &i:column_families)
            column_fam.push_back(ColumnFamilyDescriptor(i,
                ColumnFamilyOptions()));
        std::vector < ColumnFamilyHandle * > handles;
        Status s = DB::Open(DBOptions(),
                            dbPath, column_fam, &handles, &db);
        assert(s.ok());
        for (uint64_t i = 0; i < handles.size(); ++i) {
            for (uint64_t j = 0; j < fieldsCount; ++j) {
                // put and get from non-default column family
                std::string key("key");
                key += std::to_string(j);
                Slice sl(key);
                s = db->Put(WriteOptions(),
                            handles[i], key, std::to_string(j));
                assert(s.ok());
            }
        }
        // close db
        for (auto handle : handles) {
            s = db->DestroyColumnFamilyHandle(handle);
            assert(s.ok());
        }
        delete db;
    }

private:
    std::string dbPath;
    DB *db;
};

struct Family {
    std::string familyName;
    std::map <std::string, std::string> data;
};

class ConvertDataBase {
public:
    ConvertDataBase(std::string fromDbPath =
    "/Users/dellvin/Desktop/lab10/row_dellvin_db",
                    std::string toDbPath =
                    "/Users/dellvin/Desktop/lab10/hash_dellvin_db") :
            rowDbPath(fromDbPath),
            hashDbPath(std::move(toDbPath)) {}
    void convert() {
        getDataBase();
        setDataBase();
    }

private:
    void getDataBase() {
        Status s;
        DB::ListColumnFamilies(DBOptions(), rowDbPath,
                               &column_families);
        std::vector <ColumnFamilyDescriptor> column_fam;
        // open the new one, too
        for (const auto &i:column_families)
            column_fam.push_back(ColumnFamilyDescriptor(i,
                          ColumnFamilyOptions()));
        std::vector < ColumnFamilyHandle * > handles;
        s = DB::Open(DBOptions(),
                     rowDbPath, column_fam, &handles, &rowDb);
        assert(s.ok());
        boost::thread_group dbGetters;
        for (const auto &i:handles)
            dbGetters.create_thread(
                    boost::bind(&ConvertDataBase::rowWorker, this, i));
        dbGetters.join_all();
        // close db
        for (auto handle : handles) {
            s = rowDb->DestroyColumnFamilyHandle(handle);
            assert(s.ok());
        }
        delete rowDb;
    }

    void rowWorker(ColumnFamilyHandle *family) {
        muter.lock();
        rocksdb::Iterator *it = rowDb->NewIterator(
                rocksdb::ReadOptions(), family);
        muter.unlock();
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            Family f;
            f.familyName = family->GetName();
            f.data.insert(std::pair<std::string, std::string>(
                    it->key().ToString(), it->value().ToString()));
            muter.lock();
            DataBase.push(f);
            muter.unlock();
            BOOST_LOG_TRIVIAL(info) << family->GetName() << " -> "
                << it->key().ToString() << ": " << it->value().ToString()
                << std::endl;
            assert(it->status().ok());
            // Check for any errors found during the scan
        }
        delete it;
    }

    void setDataBase() {
        Options options;
        options.create_if_missing = true;
        Status s = DB::Open(options, hashDbPath, &hashDb);
        assert(s.ok());
        // create column family
        ColumnFamilyHandle *cf;

        for (const auto &i:column_families) {
            if (i != "default")
                s = hashDb->CreateColumnFamily(ColumnFamilyOptions(), i, &cf);
            assert(s.ok());
        }
        // close DB
        s = hashDb->DestroyColumnFamilyHandle(cf);
        assert(s.ok());
        delete hashDb;
        DB::ListColumnFamilies(DBOptions(), rowDbPath, &column_families);
        std::vector <ColumnFamilyDescriptor> column_fam;
        // open the new one, too
        for (const auto &i:column_families)
            column_fam.push_back(ColumnFamilyDescriptor(i,
                              ColumnFamilyOptions()));
        s = DB::Open(DBOptions(), hashDbPath, column_fam, &handles, &hashDb);
        assert(s.ok());
        boost::thread_group hashMakers;
        uint64_t FamilyNum = 0;
        for (const auto &i:column_families) {
            hashMakers.create_thread(boost::bind(&ConvertDataBase::hashWorker,
                   this, i, DataBase.size(), FamilyNum));
            FamilyNum++;
        }
        hashMakers.join_all();
        for (auto handle : handles) {
            s = hashDb->DestroyColumnFamilyHandle(handle);
            assert(s.ok());
        }
        delete hashDb;
    }

    void hashWorker(std::string familyName, uint64_t size, uint64_t FamilyNum){
        WriteBatch batch;
        uint64_t cellsCount = 0;
        for (uint64_t i = 0; i < size; ++i) {
            muter.lock();
            if (DataBase.empty()) {
                muter.unlock();
                return;
            }
            Family f = DataBase.front();
            DataBase.pop();
            if (f.familyName != familyName) {
                DataBase.push(f);
                muter.unlock();
                continue;
            }

            muter.unlock();

            const std::string hash =
                    picosha2::hash256_hex_string(f.data.begin()->second);

            muter.lock();
            batch.Put(handles[FamilyNum], f.data.begin()->first, hash);
            Status s = hashDb->Write(rocksdb::WriteOptions(), &batch);
            if (!f.data.begin()->first.empty() && !hash.empty())
                BOOST_LOG_TRIVIAL(trace) << handles[FamilyNum]->GetName()
             << " -> " << f.data.begin()->first << ": " << hash << std::endl;
            muter.unlock();
            cellsCount++;
            assert(s.ok());
        }
    }

    void init_logging() {
        logging::register_simple_formatter_factory<logging
        ::trivial::severity_level, char>("Severity");

        logging::add_file_log
                (
                        keywords::file_name = FILE_NAME,
                        keywords::rotation_size = FILE_SIZE,
                        keywords::format =
                        "[%TimeStamp%] [%ThreadID%] [%Severity%] %Message%");

        logging::add_console_log(
                std::cout,
                keywords::format =
                        "[%TimeStamp%] [%ThreadID%] [%Severity%] %Message%");

        logging::add_common_attributes();
    }


private:
    std::string rowDbPath;
    std::string hashDbPath;
    DB *rowDb;
    DB *hashDb;
    boost::mutex muter;
    std::queue <Family> DataBase;
    const std::string FILE_NAME = "log_file_%N.log";
    const unsigned FILE_SIZE = 10 * 1024 * 1024;
    std::vector<ColumnFamilyHandle *> handles;
    std::vector <std::string> column_families;
};

#endif // INCLUDE_HEADER_HPP_
