/*
** Created by Seth Shelnutt on 6/18/18.
** Licensed under the GNU Lesser General Public License v3 or later
*/

#include "client.h"
#include "curl.h"

tiledb::sm::Status get_array_schema_json_from_rest(
    std::string rest_server, std::string uri, char** jsonReturned) {
  // init the curl session
  CURL* curl = curl_easy_init();

  std::string url = std::string(rest_server) +
                    "/v1/arrays/group/group1/project/project1/uri/" +
                    curl_easy_escape(curl, uri.c_str(), uri.length());
  struct MemoryStruct memoryStruct;
  CURLcode res = get_json(curl, url, &memoryStruct);
  // Check for errors
  long httpCode = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
  if (res != CURLE_OK || httpCode >= 400) {
    // TODO: Should see if message has error json object
    return tiledb::sm::Status::Error(
        std::string("rest array get() failed: ") +
        ((memoryStruct.size > 0) ? memoryStruct.memory :
                                   " No error message from server"));
  }

  // Copy the return message
  *jsonReturned = (char*)std::malloc(memoryStruct.size * sizeof(char));
  std::memcpy(*jsonReturned, memoryStruct.memory, memoryStruct.size);

  std::free(memoryStruct.memory);
  return tiledb::sm::Status::Ok();
}

tiledb::sm::Status post_array_schema_json_to_rest(
    std::string rest_server, std::string uri, char* json) {
  // init the curl session
  CURL* curl = curl_easy_init();

  // Build the url
  std::string url = std::string(rest_server) +
                    "/v1/arrays/group/group1/project/project1/uri/" +
                    curl_easy_escape(curl, uri.c_str(), uri.length());
  struct MemoryStruct memoryStruct;
  CURLcode res = post_json(curl, url, json, &memoryStruct);
  /* Check for errors */
  long httpCode = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
  if (res != CURLE_OK || httpCode >= 400) {
    // TODO: Should see if message has error json object

    return tiledb::sm::Status::Error(
        std::string("rest array post() failed: ") +
        ((memoryStruct.size > 0) ? memoryStruct.memory :
                                   " No error message from server"));
  }

  std::free(memoryStruct.memory);
  return tiledb::sm::Status::Ok();
}
