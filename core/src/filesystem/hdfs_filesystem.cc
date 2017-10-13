/**
 * @file   hdfs_filesystem.cc
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
 * This file includes implementations of HDFS filesystem functions.
 */

#ifdef HAVE_HDFS

#include "hdfs_filesystem.h"

#include <fstream>
#include <iostream>

#include <dlfcn.h>

#include "constants.h"
#include "logger.h"
#include "utils.h"

namespace tiledb {

namespace hdfs {

// Adapted from the TensorFlow HDFS platform support code:
// https://github.com/tensorflow/tensorflow/tree/master/tensorflow/core/platform/hadoop
// copyright Tensorflow authors

Status close_library(void* handle) {
  if (dlclose(handle)) {
    return Status::Error(dlerror());
  }
  return Status::Ok();
}

Status load_library(const char* library_filename, void** handle) {
  *handle = dlopen(library_filename, RTLD_NOW | RTLD_LOCAL);
  if (!*handle) {
    return Status::Error(dlerror());
  }
  return Status::Ok();
}

Status library_symbol(void* handle, const char* symbol_name, void** symbol) {
  *symbol = dlsym(handle, symbol_name);
  if (!*symbol) {
    return Status::Error(dlerror());
  }
  return Status::Ok();
}

template <typename R, typename... Args>
Status bind_func(
    void* handle, const char* name, std::function<R(Args...)>* func) {
  void* symbol_ptr = nullptr;
  RETURN_NOT_OK(library_symbol(handle, name, &symbol_ptr));
  *func = reinterpret_cast<R (*)(Args...)>(symbol_ptr);
  return Status::Ok();
}

class LibHDFS {
 public:
  static LibHDFS* load() {
    static LibHDFS* lib = []() -> LibHDFS* {
      LibHDFS* lib = new LibHDFS;
      lib->load_and_bind();
      return lib;
    }();
    return lib;
  }

  // The status, if any, from failure to load.
  Status status() {
    return status_;
  }

  Status close() {
    return close_library(handle_);
  }

  std::function<hdfsFS(hdfsBuilder*)> hdfsBuilderConnect;
  std::function<hdfsBuilder*()> hdfsNewBuilder;
  std::function<void(hdfsBuilder*, const char*)> hdfsBuilderSetNameNode;
  std::function<int(const char*, char**)> hdfsConfGetStr;
  std::function<void(hdfsBuilder*, const char* kerbTicketCachePath)>
      hdfsBuilderSetKerbTicketCachePath;
  std::function<int(hdfsFS, hdfsFile)> hdfsCloseFile;
  std::function<tSize(hdfsFS, hdfsFile, tOffset, void*, tSize)> hdfsPread;
  std::function<tSize(hdfsFS, hdfsFile, void*, tSize)> hdfsRead;
  std::function<tSize(hdfsFS, hdfsFile, const void*, tSize)> hdfsWrite;
  std::function<int(hdfsFS, hdfsFile)> hdfsHFlush;
  std::function<int(hdfsFS, hdfsFile)> hdfsHSync;
  std::function<hdfsFile(hdfsFS, const char*, int, int, short, tSize)>
      hdfsOpenFile;
  std::function<int(hdfsFS, const char*)> hdfsExists;
  std::function<hdfsFileInfo*(hdfsFS, const char*, int*)> hdfsListDirectory;
  std::function<void(hdfsFileInfo*, int)> hdfsFreeFileInfo;
  std::function<int(hdfsFS, const char*, int recursive)> hdfsDelete;
  std::function<int(hdfsFS, const char*)> hdfsCreateDirectory;
  std::function<hdfsFileInfo*(hdfsFS, const char*)> hdfsGetPathInfo;
  std::function<int(hdfsFS, const char*, const char*)> hdfsRename;
  std::function<int(hdfsFS, hdfsFile, tOffset)> hdfsSeek;

 private:
  void load_and_bind() {
    auto try_load_bind = [this](const char* name, void** handle) -> Status {
      RETURN_NOT_OK(load_library(name, handle));
#define BIND_HDFS_FUNC(function) \
  RETURN_NOT_OK(bind_func(*handle, #function, &function));
      BIND_HDFS_FUNC(hdfsBuilderConnect);
      BIND_HDFS_FUNC(hdfsNewBuilder);
      BIND_HDFS_FUNC(hdfsBuilderSetNameNode);
      BIND_HDFS_FUNC(hdfsConfGetStr);
      BIND_HDFS_FUNC(hdfsBuilderSetKerbTicketCachePath);
      BIND_HDFS_FUNC(hdfsCloseFile);
      BIND_HDFS_FUNC(hdfsPread);
      BIND_HDFS_FUNC(hdfsRead);
      BIND_HDFS_FUNC(hdfsWrite);
      BIND_HDFS_FUNC(hdfsHFlush);
      BIND_HDFS_FUNC(hdfsHSync);
      BIND_HDFS_FUNC(hdfsOpenFile);
      BIND_HDFS_FUNC(hdfsExists);
      BIND_HDFS_FUNC(hdfsListDirectory);
      BIND_HDFS_FUNC(hdfsFreeFileInfo);
      BIND_HDFS_FUNC(hdfsDelete);
      BIND_HDFS_FUNC(hdfsCreateDirectory);
      BIND_HDFS_FUNC(hdfsGetPathInfo);
      BIND_HDFS_FUNC(hdfsRename);
      BIND_HDFS_FUNC(hdfsSeek);
#undef BIND_HDFS_FUNC
      return Status::Ok();
    };

    // libhdfs.so won't be in the standard locations. Use the path as specified
    // in the libhdfs documentation.
    char* hdfs_home = getenv("HADOOP_HOME");
    if (hdfs_home == nullptr) {
      status_ = Status::Error("Environment variable HADOOP_HOME not set");
      return;
    }
#if defined(__APPLE__)
    const char* libname = "libhdfs.dylib";
#else
    const char* libname = "libhdfs.so";
#endif
    std::string path = hdfs_home + std::string("/lib/native/") + libname;
    status_ = try_load_bind(path.c_str(), &handle_);
    if (!status_.ok()) {
      // try load libhdfs.so using dynamic loader's search path in case
      // libhdfs.so is installed in non-standard location
      status_ = try_load_bind(libname, &handle_);
    }
  }

  Status status_;
  void* handle_ = nullptr;
};

Status try_bind() {
  auto libhdfs_ = hdfs::LibHDFS::load();
  if (!libhdfs_->status().ok()) {
    std::cout << "LIBHDFS error: " << libhdfs_->status().to_string() << "\n";
  } else {
    std::cout << "LIBHDFS success!\n";
  }
  auto fs = new HDFS();
  return fs->test();
}

HDFS::HDFS()
    : libhdfs_(LibHDFS::load()) {
}

HDFS::~HDFS() {
  if (libhdfs_) {
    Status st = libhdfs_->close();
    if (!st.ok()) {
      LOG_STATUS(st);
    }
  }
}

Status HDFS::test() {
  hdfsFS fs = nullptr;
  Status st = connect(&fs);
  std::cout << "connection result: " << st.to_string() << std::endl;
  return st;
}

Status HDFS::connect(hdfsFS* fs) {
  RETURN_NOT_OK(libhdfs_->status());
  hdfsBuilder* builder = libhdfs_->hdfsNewBuilder();
  // TODO: allow customizing namenode
  libhdfs_->hdfsBuilderSetNameNode(builder, "default");
  *fs = libhdfs_->hdfsBuilderConnect(builder);
  if (*fs == nullptr) {
    return Status::Error(strerror(errno));
  }
  return Status::Ok();
}

Status HDFS::create_dir(const URI& uri) {
  std::cout << "DEBUG: " << __func__ << std::endl;
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));
  if (is_dir(uri)) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot create directory ") + uri.to_string() +
        "'; Directory already exists"));
  }
  int ret = libhdfs_->hdfsCreateDirectory(fs, uri.to_path().c_str());
  if (ret < 0) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot create directory ") + uri.to_string()));
  }
  return Status::Ok();
}

// delete the directory with the given path
Status HDFS::delete_dir(const URI& uri) {
  std::cout << "DEBUG: " << __func__ << std::endl;
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));

  int ret = libhdfs_->hdfsDelete(fs, uri.to_path().c_str(), 1);
  if (ret < 0) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot delete directory '") + uri.to_string()));
  }
  return Status::Ok();
}

Status HDFS::move_dir(const URI& old_uri, const URI& new_uri) {
  std::cout << "DEBUG: " << __func__ << std::endl;
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));

  int ret = libhdfs_->hdfsRename(
      fs, old_uri.to_path().c_str(), new_uri.to_path().c_str());
  if (ret < 0) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot move directory ") + old_uri.to_string() + " to " +
        new_uri.to_string()));
  }
  return Status::Ok();
}

bool HDFS::is_dir(const URI& uri) {
  std::cout << "DEBUG: " << __func__ << std::endl;
  hdfsFS fs = nullptr;
  Status st = connect(&fs);
  if (!st.ok()) {
    return false;
  }
  int exists = libhdfs_->hdfsExists(fs, uri.to_path().c_str());
  if (exists == 0) {  // success
    hdfsFileInfo* fileInfo =
        libhdfs_->hdfsGetPathInfo(fs, uri.to_path().c_str());
    if (fileInfo == nullptr) {
      return false;
    }
    if ((char)(fileInfo->mKind) == 'D') {
      libhdfs_->hdfsFreeFileInfo(fileInfo, 1);
      return true;
    } else {
      libhdfs_->hdfsFreeFileInfo(fileInfo, 1);
      return false;
    }
  }
  return false;
}

bool HDFS::is_file(const URI& uri) {
  std::cout << "DEBUG: " << __func__ << std::endl;
  hdfsFS fs = nullptr;
  Status st = connect(&fs);
  if (!st.ok()) {
    return false;
  }

  int ret = libhdfs_->hdfsExists(fs, uri.to_path().c_str());
  if (!ret) {
    hdfsFileInfo* fileInfo =
        libhdfs_->hdfsGetPathInfo(fs, uri.to_path().c_str());
    if (fileInfo == NULL) {
      return false;
    }
    if ((char)(fileInfo->mKind) == 'F') {
      libhdfs_->hdfsFreeFileInfo(fileInfo, 1);
      return true;
    } else {
      libhdfs_->hdfsFreeFileInfo(fileInfo, 1);
      return false;
    }
  }
  return false;
}

Status HDFS::create_file(const URI& uri) {
  std::cout << "DEBUG: " << __func__ << std::endl;
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));

  // Open file
  hdfsFile writeFile =
      libhdfs_->hdfsOpenFile(fs, uri.to_path().c_str(), O_WRONLY, 0, 0, 0);
  if (!writeFile) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot create file ") + uri.to_string() +
        "; File opening error"));
  }
  // Close file
  if (libhdfs_->hdfsCloseFile(fs, writeFile)) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot create file ") + uri.to_string() +
        "; File closing error"));
  }
  return Status::Ok();
}

Status HDFS::delete_file(const URI& uri) {
  std::cout << "DEBUG: " << __func__ << std::endl;
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));
  int ret = libhdfs_->hdfsDelete(fs, uri.to_path().c_str(), 0);
  if (ret < 0) {
    return LOG_STATUS(
        Status::IOError(std::string("Cannot delete file ") + uri.to_string()));
  }
  return Status::Ok();
}

Status HDFS::read_from_file(
    const URI& uri, off_t offset, void* buffer, uint64_t length) {
  std::cout << "DEBUG: " << __func__ << std::endl;
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));
  std::cout << "DEBUG: " << __func__ << " CONNECTED " << std::endl;
  hdfsFile readFile =
      libhdfs_->hdfsOpenFile(fs, uri.to_path().c_str(), O_RDONLY, length, 0, 0);
  std::cout << "DEBUG: " << __func__ << " OPENED " << std::endl;
  if (!readFile) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot read file ") + uri.to_string() +
        ": file open error"));
  }
  int ret = libhdfs_->hdfsSeek(fs, readFile, (tOffset)offset);
  std::cout << "DEBUG: " << __func__ << " SEEK " << std::endl;
  if (ret < 0) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot seek to offset ") + uri.to_string()));
  }
  uint64_t bytes_to_read = length;
  char* buffptr = static_cast<char*>(buffer);
  do {
    tSize nbytes = (bytes_to_read <= INT_MAX) ? bytes_to_read : INT_MAX;
    tSize bytes_read =
        libhdfs_->hdfsRead(fs, readFile, static_cast<void*>(buffptr), nbytes);
    if (bytes_read < 0) {
      return LOG_STATUS(Status::IOError(
          "Cannot read from file " + uri.to_string() + "; File reading error"));
    }
    bytes_to_read -= bytes_read;
    buffptr += bytes_read;
    assert(bytes_read >= 0);
  } while (bytes_to_read > 0);

  std::cout << "DEBUG: " << __func__ << " READ " << std::endl;
  // Close file
  if (libhdfs_->hdfsCloseFile(fs, readFile)) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot read from file ") + uri.to_string() +
        "; File closing error"));
  }
  std::cout << "DEBUG: " << __func__ << " CLOSE " << std::endl;
  return Status::Ok();
}

Status HDFS::write_to_file(
    const URI& uri, const void* buffer, const uint64_t length) {
  std::cout << "DEBUG: " << __func__ << std::endl;
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));
  int flags = is_file(uri) ? O_WRONLY | O_APPEND : O_WRONLY;
  hdfsFile writeFile = libhdfs_->hdfsOpenFile(
      fs, uri.to_path().c_str(), flags, constants::max_write_bytes, 0, 0);
  if (!writeFile) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot write to file ") + uri.to_string() +
        "; File opening error"));
  }
  // Append data to the file in batches of Configurator::max_write_bytes()
  // bytes at a time
  // ssize_t bytes_written = 0;
  off_t nrRemaining = 0;
  tSize curSize = 0;
  tSize written = 0;
  for (nrRemaining = (off_t)length; nrRemaining > 0;
       nrRemaining -= constants::max_write_bytes) {
    curSize = (constants::max_write_bytes < nrRemaining) ?
                  constants::max_write_bytes :
                  static_cast<tSize>(nrRemaining);
    if ((written = libhdfs_->hdfsWrite(fs, writeFile, buffer, curSize)) !=
        curSize) {
      return LOG_STATUS(Status::IOError(
          std::string("Cannot write to file ") + uri.to_string() +
          "; File writing error"));
    }
  }
  // Close file
  if (libhdfs_->hdfsCloseFile(fs, writeFile)) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot write to file ") + uri.to_string() +
        "; File closing error"));
  }
  return Status::Ok();
}

Status HDFS::ls(const URI& uri, std::vector<std::string>* paths) {
  std::cout << "DEBUG: " << __func__ << std::endl;
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));

  int numEntries = 0;
  hdfsFileInfo* fileList =
      libhdfs_->hdfsListDirectory(fs, uri.to_path().c_str(), &numEntries);
  if (fileList == NULL) {
    if (errno) {
      return LOG_STATUS(Status::IOError(
          std::string("Cannot list files in ") + uri.to_string()));
    }
  }
  for (int i = 0; i < numEntries; ++i) {
    auto path = std::string(fileList[i].mName);
    if (!utils::starts_with("hdfs://", path)) {
      path = std::string("hdfs://") + path;
    }
    paths->push_back(path);
  }
  libhdfs_->hdfsFreeFileInfo(fileList, numEntries);
  return Status::Ok();
}

Status HDFS::file_size(const URI& uri, uint64_t* nbytes) {
  std::cout << "DEBUG: " << __func__ << std::endl;
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));

  hdfsFileInfo* fileInfo = libhdfs_->hdfsGetPathInfo(fs, uri.to_path().c_str());
  if (fileInfo == NULL) {
    return LOG_STATUS(
        Status::IOError(std::string("Not a file ") + uri.to_string()));
  }
  if ((char)(fileInfo->mKind) == 'F') {
    *nbytes = static_cast<uint64_t>(fileInfo->mSize);
  } else {
    libhdfs_->hdfsFreeFileInfo(fileInfo, 1);
    return LOG_STATUS(
        Status::IOError(std::string("Not a file ") + uri.to_string()));
  }
  libhdfs_->hdfsFreeFileInfo(fileInfo, 1);
  return Status::Ok();
}

}  // namespace hdfs

}  // namespace tiledb

#endif
