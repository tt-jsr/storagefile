#pragma once

#ifndef STORAGE_FILE_H_INCLUDED
#define STORAGE_FILE_H_INCLUDED

#include "boost/noncopyable.hpp"
#include "records.h"

namespace transactioncounting_ns
{
    struct StoragePosition
    {
    public:
        long Offset() const {return offset;}        // Offset of this record
        int RecordId() const {return recordId;}     // Application specified recordid
        size_t Size() const {return size;}          // Size of the data
        bool End() const {return offset == 0;}      // EOF, this is not a valid position
        _int64 Created() {return created;}          // Created time in FILETIME format
        _int64   Modified() { return modified;}     // Last update in FILETIME format
    private:
        long offset;
        int recordId;
        size_t size;
        _int64 created;
        _int64 modified;
        friend class StorageFile;
    };

    namespace detail
    {
        struct FileHeader;
        struct RecordHeader;
    }

    class StorageFile : private boost::noncopyable
    {
    public:
        StorageFile();
        ~StorageFile();

        // Open a storage file.
        // fileId is a 31 char field identifying the application file type. If the file
        // is being opened, it will fail if the id does not match what is stored in the file.
        // This will create the file if it does not exist.
        bool Open(const std::string& file, bool readOnly, const char * fileId);

        //! Close the file
        bool Close();

        // Add a new record.
        // data: pointer to the data to be written
        // size: the size of the data
        // recordId: An application defined id to identify the record type
        // offset: will contain the offset of the record when it returns
        // throws std::exception
        void AddRecord(const void *data, size_t size, int recordId, StoragePosition& pos /* out */);

        // Update a record
        // data: pointer to the data to be written
        // size: The size of the data to be written
        // recordId: An application defined record type. This function will fail if
        //           the record type does not match the existing record
        // throws std::exception
        void UpdateRecord(const void *data, size_t size, const StoragePosition& pos);

        // Read a record
        // throws std::exception
        void ReadRecord(void *data, size_t size, const StoragePosition& pos);

        // Iterate through the counts of the current or past session
        StoragePosition Begin();
        StoragePosition Begin(long offset);
        bool Next(StoragePosition& pos);

    private:
        bool WriteFileHeader(const detail::FileHeader&);
        bool ReadFileHeader(detail::FileHeader&);
        void ReadRecordHeader(const StoragePosition& pos, detail::RecordHeader&);
        void WriteRecordHeader(const StoragePosition& pos, const detail::RecordHeader&);
        void ValidateRecordHeader(const StoragePosition& pos);
    private:
        int m_fd;
        bool m_bReadOnly;
        std::string m_filepath;
        long m_fileLength;
    };
}

#endif // STORAGE_FILE_H_INCLUDED

