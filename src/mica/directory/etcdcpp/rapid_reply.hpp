/*

 The MIT License (MIT)

 Copyright (C) 2015 Suryanathan Padmanabhan

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 THE SOFTWARE.

*/

#ifndef __RAPID_REPLY_HPP_INCLUDED__
#define __RAPID_REPLY_HPP_INCLUDED__

#include <iostream>
#include "etcd.hpp"

// JSON PARSER INCLUDES
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"

namespace example {
/////////////////////////////////////////////////////////////////////////////

/**
 * @brief An example json reply wrapper. This can be easily replaced with a user
 * defined wrapper as song as it implements a similar interface
 */
class RapidReply {
  public:
    // Types
    typedef std::map<std::string, std::string> KvPairs;

    // LIFECYCLE
    RapidReply(const std::string& reply)
      :document_(),
       header_() {
        _Parse(reply);
    }

    RapidReply(
        const std::string& header,
        const std::string& reply)
     :document_(),
      header_(header) {
        _Parse(reply);
    }

    // OPERATIONS
    void Print() const {
        rapidjson::StringBuffer strbuf;
        rapidjson::Writer<rapidjson::StringBuffer> writer(strbuf);
        document_.Accept(writer);
        std::cerr << strbuf.GetString() << '\n';
    }

    void GetAll(KvPairs& kvPairs) {
        if (! document_.HasMember(kNode)) {
        }
        //ToDo check whether prefix should be "/"
        return _GetAll(document_[kNode], kvPairs);
    }

    etcd::Action GetAction() {
        if (! document_.HasMember(kAction)) {
            return etcd::Action::UNKNOWN;
        }
        CAM_II iter = kActionMap.find (document_[kAction].GetString());
        if (iter == kActionMap.end()) {
            return etcd::Action::UNKNOWN;
        }
        return iter->second;
    }

    etcd::Index GetModifiedIndex() const {
        if ((! document_.HasMember(kNode)) ||
            (!document_[kNode].HasMember(kModifiedIndex))) {
            throw std::runtime_error("possibly timed out");
        }
        return document_[kNode][kModifiedIndex].GetUint64();
    }

  private:
    // TYPES
    typedef etcd::ResponseActionMap::const_iterator CAM_II;

    // CONSTANTS
    const char *kErrorCode ="errorCode";
    const char *kMessage = "message";
    const char *kCause ="cause";
    const char *kNode = "node";
    const char *kModifiedIndex = "modifiedIndex";
    const char *kNodes = "nodes";
    const char *kDir = "dir";
    const char *kKey = "key";
    const char *kValue = "value";
    const char* kAction = "action";
    const etcd::ResponseActionMap kActionMap;

    // DATA MEMBERS
    rapidjson::Document document_;
    std::string header_;

    // OPERATIONS
    void _CheckError() { 
        if (document_.HasMember(kErrorCode)) {
            throw etcd::ReplyException(document_[kErrorCode].GetInt(),
                    document_[kMessage].GetString(),
                    document_[kCause].GetString());
        }
    }

    void _Parse(const std::string& json) {
        document_.Parse(json.c_str());
        _CheckError();
    }

    void _GetAll(const rapidjson::Value& doc, KvPairs& kvPairs) {
        if (doc.HasMember(kDir) && (doc[kDir].GetBool() == true)) {
            if (!doc.HasMember(kNodes))
                return;  // directory doesn't have nodes
            const rapidjson::Value& nodes = doc[kNodes];
            assert(nodes.IsArray());
            for (size_t i = 0; i < nodes.Size(); ++i)
                _GetAll(nodes[i], kvPairs);
        } else {
            if (doc.HasMember(kKey)) {
                if (doc.HasMember(kValue)) {
                    kvPairs.emplace(doc[kKey].GetString(), doc[kValue].GetString());
                } else {
                    kvPairs.emplace(doc[kKey].GetString(), "");
                }
            }
        }
    }
};

} // namespace example

#endif // __RAPID_REPLY_HPP_INCLUDED__
