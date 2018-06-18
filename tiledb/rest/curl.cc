/**
 * @file   client.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file implements a curl client helper functions.
 */
#include "curl.h"
#include <cstring>

/**
 * @Brief Call back for saving libcurl response body
 *
 * @param contents
 * @param size
 * @param nmemb
 * @param userp
 * @return
 */
size_t WriteMemoryCallback(
    void* contents, size_t size, size_t nmemb, void* userp) {
  size_t realsize = size * nmemb;
  struct MemoryStruct* mem = (struct MemoryStruct*)userp;

  mem->memory = (char*)realloc(mem->memory, mem->size + realsize + 1);
  if (mem->memory == NULL) {
    /* out of memory! */
    printf("not enough memory (realloc returned NULL)\n");
    return 0;
  }

  memcpy(&(mem->memory[mem->size]), contents, realsize);
  mem->size += realsize;
  mem->memory[mem->size] = 0;

  return realsize;
}

/**
 * @brief fetch and return url body via curl
 * @param curl
 * @param url
 * @param fetch
 * @return
 */
CURLcode curl_fetch_url(
    CURL* curl, const char* url, struct MemoryStruct* fetch) {
  CURLcode rcode; /* curl result code */

  /* init memory */
  fetch->memory = (char*)calloc(1, sizeof(fetch->memory));

  /* check memory */
  if (fetch->memory == NULL) {
    /* log error */
    fprintf(stderr, "ERROR: Failed to allocate memory in curl_fetch_url");
    /* return error */
    return CURLE_FAILED_INIT;
  }

  /* init size */
  fetch->size = 0;

  /* set url to fetch */
  curl_easy_setopt(curl, CURLOPT_URL, url);

  /* set calback function */
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);

  /* pass fetch struct pointer */
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void*)fetch);

  /* set default user agent */
  // curl_easy_setopt(curl, CURLOPT_USERAGENT, "libcurl-agent/1.0");

  /* Fail on error */
  // curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);

  /* set timeout */
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5);

  /* enable location redirects */
  curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);

  /* set maximum allowed redirects */
  curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 1);

  /* fetch the url */
  rcode = curl_easy_perform(curl);

  /* return */
  return rcode;
}

CURLcode post_json(
    CURL* curl,
    std::string url,
    std::string jsonString,
    MemoryStruct* memoryStruct) {
  /* HTTP PUT please */
  curl_easy_setopt(curl, CURLOPT_POST, 1L);

  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, jsonString.c_str());

  struct curl_slist* headers = NULL;
  headers = curl_slist_append(headers, "Content-Type: application/json");

  /* pass our list of custom made headers */
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  return curl_fetch_url(curl, url.c_str(), memoryStruct);
}

CURLcode get_json(CURL* curl, std::string url, MemoryStruct* memoryStruct) {
  struct curl_slist* headers = NULL;
  headers = curl_slist_append(headers, "Content-Type: application/json");

  /* pass our list of custom made headers */
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  return curl_fetch_url(curl, url.c_str(), memoryStruct);
}
