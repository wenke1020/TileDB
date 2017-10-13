/**
 * @file   hdfs_filesystem.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file includes declarations of HDFS filesystem functions.
 */

#ifndef TILEDB_FILESYSTEM_HDFS_H
#define TILEDB_FILESYSTEM_HDFS_H

#include <sys/types.h>
#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <memory>

#include "buffer.h"
#include "status.h"
#include "uri.h"

#include "hadoop/include/hdfs.h"

namespace tiledb {

namespace hdfs {

class LibHDFS;

class HdfsFSCache {
  public:
    typedef std::map<std::string, hdfsFS> HdfsFSMap;

    static HdfsFSCache* instance() { return HdfsFSCache::instance_.get(); }
    static Status init();

    Status get_local_connection(hdfsFS* fs);
    Status get_connection(LibHDFS* libhdfs, hdfsFS* fs);

  private:
    static std::unique_ptr<HdfsFSCache> instance_;
    
    std::mutex lock_;
    HdfsFSMap fs_map_;

    HdfsFSCache() {}
    HdfsFSCache(HdfsFSCache const& l); // disable copy ctor
    HdfsFSCache& operator=(HdfsFSCache const& l); // disable assignment 
};

class HDFS {
 public:
  HDFS();
  ~HDFS();

  Status test();

  Status create_dir(const URI& uri);

  Status delete_dir(const URI& uri);

  bool is_dir(const URI& uri);

  Status move_dir(const URI& old_uri, const URI& new_uri);

  bool is_file(const URI& uri);

  Status create_file(const URI& uri);

  Status delete_file(const URI& uri);

  Status read_from_file(
      const URI& uri, off_t offset, void* buffer, uint64_t length);

  Status write_to_file(
      const URI& uri, const void* buffer, const uint64_t length);

  Status ls(const URI& uri, std::vector<std::string>* paths);

  Status file_size(const URI& uri, uint64_t* nbytes);

 private:
  Status connect(hdfsFS* fs);
  LibHDFS* libhdfs_;
};

}  // namespace hdfs

}  // namespace tiledb

#endif  // TILEDB_FILESYSTEM_HDFS_H
