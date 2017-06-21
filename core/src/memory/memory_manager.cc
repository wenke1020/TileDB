/**
 * @file   memory_manager.cc
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
 * This file implements the MemoryManager class.
 */

#include "memory_manager.h"
#include "utils.h"

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#ifdef TILEDB_VERBOSE
#define PRINT_ERROR(x) std::cerr << TILEDB_MM_ERRMSG << x << ".\n"
#else
#define PRINT_ERROR(x) \
  do {                 \
  } while (0)
#endif

/* ****************************** */
/*        GLOBAL VARIABLES        */
/* ****************************** */

std::string tiledb_mm_errmsg = "";

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

MemoryManager::MemoryManager() {
  // Initializations
  malloc_ = &default_malloc;
  realloc_ = &default_realloc;
  free_ = &default_free;
}

MemoryManager::~MemoryManager() {
}

/* ********************************* */
/*                API                */
/* ********************************* */

int MemoryManager::set_allocators(
    void* (*malloc)(uint64_t, void*),
    void* (*realloc)(void*, uint64_t, void*),
    void (*free)(void*, void*)) {
  // Set allocators
  malloc_ = malloc;
  realloc_ = realloc;
  free_ = free;

  // Success
  return TILEDB_MM_OK;
}
