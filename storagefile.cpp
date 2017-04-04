/***************************************************************************
 *    
 *                Unpublished Work Copyright (c) 2012
 *                  Trading Technologies International, Inc.
 *                       All Rights Reserved Worldwide
 *
 *          * * *   S T R I C T L Y   P R O P R I E T A R Y   * * *
 *
 * WARNING:  This program (or document) is unpublished, proprietary property
 * of Trading Technologies International, Inc. and is to be maintained in
 * strict confidence. Unauthorized reproduction, distribution or disclosure 
 * of this program (or document), or any program (or document) derived from
 * it is prohibited by State and Federal law, and by local law outside of 
 * the U.S.
 *
 ***************************************************************************
 * $Source: storagefile.cpp $
 * $Date: 2012/04/26 10:31:30CDT $
 * $Author: Jeff Richards (TT) (jrichards) $
 * $Revision: 1.1 $
 ***************************************************************************/
#include "pch_transactioncounting.h"
#include "storagefile.h"
#include <io.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

using namespace std;
using namespace tt_core_ns;

//TODO: Delete this
#define LOGID 999

namespace
{
    enum
    {
        HEADER_MAGIC = 0x3f342287,
        HEADER_VERSION = 1,
        MAX_FILEID = 32
    };

    _int64 _GetTime()
    {
        FILETIME ft;
        GetSystemTimeAsFileTime(&ft);   // UTC
        ULARGE_INTEGER uli;
        uli.LowPart = ft.dwLowDateTime;
        uli.HighPart = ft.dwHighDateTime;
        return uli.QuadPart;
    }
}

namespace transactioncounting_ns
{
    namespace detail
    {
        struct FileHeader
        {
            int magic;
            int version;
            char fileid[MAX_FILEID];    // including NULL
        };

        struct RecordHeader
        {
            size_t size;
            int recordId;
            _int64 created;
            _int64 modified;
        };
    }

    StorageFile::StorageFile()
        :m_fd(-1)
        , m_bReadOnly(false)
    {
    }

    StorageFile::~StorageFile()
    {
        Close();
        return;
    }

    bool StorageFile::Open(const std::string& file, bool bReadOnly, const char *fileId)
    {
        m_bReadOnly = bReadOnly;
        if (m_fd > 0)
        {
            tt_log::GetCustomLog().Log( tt_log::SV_ERROR, LOGID, "[TransactionCounting] Opened failed, %s is already opened", file.c_str());
            return false;
        }
        if (m_bReadOnly)
        {
            m_fd = _open(file.c_str(), _O_RDONLY | _O_BINARY);
        }
        else
        {
            if (_access(file.c_str(), 0) < 0)
            {
                // file does not exist
                m_fd = _open(file.c_str(), _O_RDWR | _O_CREAT | _O_TRUNC | _O_BINARY, _S_IREAD | _S_IWRITE);
                if (m_fd < 0)
                {
                    tt_log::GetCustomLog().Log( tt_log::SV_ERROR, LOGID, "[TransactionCounting] Error creating %s error: %s.",  file.c_str(), _sys_errlist[errno] );
                    return false;
                }
                detail::FileHeader hdr;
                memset(&hdr, 0, sizeof(hdr));
                hdr.magic = HEADER_MAGIC;
                hdr.version = HEADER_VERSION;
                strncpy(hdr.fileid, fileId, MAX_FILEID);
                hdr.fileid[MAX_FILEID-1] = '\0';
                WriteFileHeader(hdr);
            }
            else
            {
                m_fd = _open(file.c_str(), _O_RDWR | _O_BINARY);
            }
        }
        if (m_fd < 0)
        {
            tt_log::GetCustomLog().Log( tt_log::SV_ERROR, LOGID, "[TransactionCounting] Error opening %s error: %s.",  file.c_str(), _sys_errlist[errno] );
            return false;
        }
        detail::FileHeader hdr;
        if (false == ReadFileHeader(hdr))
        {
            tt_log::GetCustomLog().Log( tt_log::SV_ERROR, LOGID, "[TransactionCounting] %s is not a storage file", file.c_str());
            return false;
        }
        if (hdr.magic != HEADER_MAGIC)
        {
            tt_log::GetCustomLog().Log( tt_log::SV_ERROR, LOGID, "[TransactionCounting] %s is not a storage file", file.c_str());
            return false;
        }
        if (hdr.version != HEADER_VERSION)
        {
            tt_log::GetCustomLog().Log( tt_log::SV_ERROR, LOGID, "[TransactionCounting] %s  unsupported version, expected %d got %d", file.c_str(), HEADER_VERSION, hdr.version);
            return false;
        }
        if (strncmp(hdr.fileid, fileId, MAX_FILEID) != 0)
        {
            tt_log::GetCustomLog().Log( tt_log::SV_ERROR, LOGID, "[TransactionCounting] %s  fileid mispatch, expected %d got %d", file.c_str(), fileId, hdr.fileid);
            return false;
        }

        m_fileLength = _lseek(m_fd, 0, SEEK_END);
        return true;
    }

    bool StorageFile::Close()
    {
        if (m_fd < 0)
        {
            return true;
        }
        _close(m_fd);
        m_fd = -1;
        return true;
    }

    void StorageFile::AddRecord(const void *data, size_t size, int recordId, StoragePosition& pos)
    {
        TT_ASSERT(m_fd > 0);
        TT_ASSERT(m_bReadOnly == false);

        pos.offset = _lseek(m_fd, 0, SEEK_END);

        detail::RecordHeader rh;
        rh.recordId = recordId;
        rh.size = size;
        rh.modified = 0;
        rh.created = _GetTime();
        pos.recordId = recordId;
        pos.size = size;
        _write(m_fd, &rh, sizeof(detail::RecordHeader));
        _write(m_fd, data, size);
        m_fileLength = _lseek(m_fd, 0, SEEK_END);
    }


    void StorageFile::ReadRecord(void *data, size_t size, const StoragePosition& pos)
    {
        TT_ASSERT(m_fd > 0);
        TT_ASSERT(pos.offset < m_fileLength);

#if defined(_DEBUG)
        ValidateRecordHeader(pos);
#endif
        _lseek(m_fd, pos.offset + sizeof(detail::RecordHeader), SEEK_SET);
        int r = _read(m_fd, data, size);
        if (r < 0)
        {
            stringstream strm;
            strm << "Read error: " << _sys_errlist[errno];
            throw std::runtime_error(strm.str());
        }
        if ((size_t)r != size)
        {
            throw std::runtime_error("Unexpected EOF");
        }
    }

    void StorageFile::UpdateRecord(const void *data, size_t size, const StoragePosition& pos)
    {
        TT_ASSERT(m_fd > 0);
        TT_ASSERT(m_bReadOnly == false);
        TT_ASSERT(pos.offset < m_fileLength);
        detail::RecordHeader rh;
        ReadRecordHeader(pos, rh);

        if (pos.size != size)
        {
            throw std::runtime_error("Size does not match record size");
        }
        rh.modified = _GetTime();
        WriteRecordHeader(pos, rh);
        _lseek(m_fd, pos.offset+sizeof(detail::RecordHeader), SEEK_SET);

        _write(m_fd, data, size);
    }


    bool StorageFile::WriteFileHeader(const detail::FileHeader& hdr)
    {
        TT_ASSERT(m_fd > 0);
        TT_ASSERT(m_bReadOnly == false);
        if (m_bReadOnly)
        {
            return false;
        }
        _lseek(m_fd, 0, SEEK_SET);
        int r =_write(m_fd, &hdr, sizeof(hdr));
        TT_ASSERT(r == sizeof(hdr));
        if (r != sizeof(hdr))
        {
            return false;
        }
        return true;
    }

    bool StorageFile::ReadFileHeader(detail::FileHeader& hdr)
    {
        TT_ASSERT(m_fd > 0);
        _lseek(m_fd, 0, SEEK_SET);
        int r = _read(m_fd, &hdr, sizeof(hdr));
        if (r != sizeof(hdr))
        {
            return false;
        }
        hdr.fileid[MAX_FILEID-1] = '\0';
        return true;
    }

    void StorageFile::WriteRecordHeader(const StoragePosition& pos, const detail::RecordHeader& hdr)
    {
        TT_ASSERT(m_fd > 0);
        _lseek(m_fd, pos.offset, SEEK_SET);
        _write(m_fd, &hdr, sizeof(hdr));
    }

    void StorageFile::ReadRecordHeader(const StoragePosition& pos, detail::RecordHeader& hdr)
    {
        TT_ASSERT(m_fd > 0);
        _lseek(m_fd, pos.offset, SEEK_SET);
        int r = _read(m_fd, &hdr, sizeof(hdr));
        if (r < 0)
        {
            stringstream strm;
            strm << "Read error: " << _sys_errlist[errno];
            throw std::runtime_error(strm.str());
        }
        if (r != sizeof(hdr))
        {
            throw std::runtime_error("Unexpected EOF");
        }
        TT_ASSERT(hdr.recordId == pos.recordId);
        TT_ASSERT(hdr.size == pos.size);
    }

    StoragePosition StorageFile::Begin(long offset)
    {
        TT_ASSERT(offset < m_fileLength);
        StoragePosition pos;
        if (offset >= m_fileLength)
        {
            pos.offset = 0;
            pos.recordId = 0;
            pos.size = 0;
            return pos;
        }
        pos.offset = _lseek(m_fd, offset, SEEK_SET);
        detail::RecordHeader rh;
        _read(m_fd, &rh, sizeof(rh));
        pos.recordId = rh.recordId;
        pos.size = rh.size;
        return pos;
    }

    StoragePosition StorageFile::Begin()
    {
        TT_ASSERT(m_fd > 0);
        StoragePosition pos;
        pos.offset = _lseek(m_fd, sizeof(detail::FileHeader), SEEK_SET);
        if (pos.offset == m_fileLength)
        {
            pos.offset = 0;
            pos.recordId = 0;
            pos.size = 0;
            pos.created = 0;
            pos.modified = 0;
            return pos;
        }
        detail::RecordHeader rh;
        ReadRecordHeader(pos, rh);
        pos.recordId = rh.recordId;
        pos.size = rh.size;
        return pos;
    }

    bool StorageFile::Next(StoragePosition& pos)
    {
        TT_ASSERT(m_fd > 0);
        TT_ASSERT(pos.offset != 0);
        if (pos.offset == 0)
        {
            return false;
        }
        pos.offset = pos.offset + sizeof(detail::RecordHeader) + pos.size;
        if (pos.offset >= m_fileLength)
        {
            pos.offset = 0;
            pos.recordId = 0;
            pos.size = 0;
            pos.created = 0;
            pos.modified = 0;
            return false;   // eof
        }
        detail::RecordHeader rh;
        ReadRecordHeader(pos, rh);
        pos.recordId = rh.recordId;
        pos.created = rh.created;
        pos.modified = rh.modified;
        pos.size = rh.size;

        return true;
    }

    void StorageFile::ValidateRecordHeader(const StoragePosition& pos)
    {
        _lseek(m_fd, pos.offset, SEEK_SET);
        detail::RecordHeader rh;
        int r = _read(m_fd, &rh, sizeof(rh));
        if (r < 0)
        {
            stringstream strm;
            strm << "Read error: " << _sys_errlist[errno];
            throw std::runtime_error(strm.str());
        }
        if (r != sizeof(rh))
        {
            throw std::runtime_error("Unexpected EOF");
        }
        TT_ASSERT(rh.recordId == pos.recordId);
        TT_ASSERT(rh.size == pos.size);
    }
}

