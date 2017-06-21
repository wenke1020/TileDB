/**
 * @file   memory_manager.h
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
 * This file defines class MemoryManager.
 */

#ifndef __MEMORY_MANAGER_H__
#define __MEMORY_MANAGER_H__

#include <string>

/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

/**@{*/
/** Return code. */
#define TILEDB_MM_OK 0
#define TILEDB_MM_ERR -1
/**@}*/

/** Default error message. */
#define TILEDB_MM_ERRMSG std::string("[TileDB::MemoryManager] Error: ")

/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

/** Stores potential error messages. */
extern std::string tiledb_mm_errmsg;

/** The MemoryManager class. */
class MemoryManager {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  MemoryManager();

  /** Destructor. */
  ~MemoryManager();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Sets the custom memory allocators.
   *
   * @param malloc The allocator.
   * @param realloc The reallocator.
   * @param free The deallocator.
   * @return TILEDB_MM_OK for success and TILEDB_MM_ERR for error.
   *
   * @note Allocators should not be set more than once.
   */
  int set_allocators(
      void* (*malloc)(uint64_t, void*),
      void* (*realloc)(void*, uint64_t, void*),
      void (*free)(void*, void*));

 private:
  /* ********************************* */
  /*          PRIVATE MEMBERS          */
  /* ********************************* */

  /**
   * Allocates memory.
   *
   * @param size The size (in bytes) of the memory to be allocated.
   * @param data Auxiliary data.
   * @return The pointer to the newly allocated memory.
   */
  void* (*malloc_)(uint64_t size, void* data);

  /**
   * Reallocates memory.
   *
   * @param p The pointer of the memory to be reallocated.
   * @param size The size of the memory to be reallocated.
   * @param data Auxiliary data.
   * @return The pointer to the reallocated memory.
   */
  void* (*realloc_)(void* p, uint64_t size, void* data);

  /**
   * Deallocates memory.
   *
   * @param p The pointer of the memory to be freed.
   * @param data Auxiliary data.
   * @return void
   */
  void (*free_)(void* p, void* data);

  /** The allocated size so far. */
  uint64_t alloced_size_;
};

#endif