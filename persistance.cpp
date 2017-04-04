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
 * $Source: persistance.cpp $
 * $Date: 2012/04/26 10:31:29CDT $
 * $Author: Jeff Richards (TT) (jrichards) $
 * $Revision: 1.1 $
 ***************************************************************************/
#include "pch_transactioncounting.h"
#include "persistance.h"
#include "records.h"
#include "storagefile.h"
#include <io.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

//TODO: Delete this
#define LOGID 999

using namespace std;
using namespace tt_core_ns;

namespace
{
    enum 
    {
        MAXIMUM_ROUTING_KEY_OUTPUT=32,
        MAXIMUM_TRANSACTION_IDENTIFIER_OUTPUT = 64
    };

    string TCFormatOrder( const Order& order )
    {
        char buffer[ 1024 ];
        size_t bufferSize = sizeof( buffer );

#ifdef _DEBUG
        if(  !IsValidExchangeId( order.srs.prod.srs_exch_id ))
        {
            TT_ASSERT( "FormatOrder: invalid ExchangeId" );
        }
#endif 
        TTQtyType executedQuantity = max( order.exec_qty, order.fill_qty );

        string limitPriceString;
        try
        {
            limitPriceString = tt_core_ns::TTTick::PriceIntToString( order.limit_prc, &order.srs );
        }
        catch( tt_core_ns::TTTick::Ex& exception )
        {
            stringstream limitPriceStream;
            limitPriceStream << order.limit_prc;
            limitPriceString.assign( limitPriceStream.str() );
            cout << "Exception1 in FormatOrder(): " << exception.what() << endl;
        }

        string stopPriceString;
        try
        {
            stopPriceString = tt_core_ns::TTTick::PriceIntToString(order.stop_prc, &order.srs);
        }
        catch( tt_core_ns::TTTick::Ex& exception )
        {
            cout << "Exception2 in FormatOrder(): " << exception.what() << endl;
        }

        char orderNoStr[64];
        SysTTUInt64toA(order.order_no, orderNoStr, 10);

        string s = order.GetRoutingKey();
        enum {MAXIMUM_ROUTING_KEY_OUTPUT=32};
        SNPRINTF( buffer, bufferSize, 
            "sts:%04d,%c,%c,%c,%c,%s,%s,%s,%c,%c,%d,%d,%d,%d,%d,%s,%s%s%s,%s%s%s,%.*s,%d,%d,%s,%s,%s,%s,%s,%s,%s,%s,%d,%d,%d,%d,%d",
            order.order_status.ToInt(), ( order.m_isIncomplete ? 'I' : 'C' ),
            order.order_action.ToChar(),
            order.buy_sell.ToChar(),
            order.open_close.ToChar(),
            FormatSeries( order.srs ).c_str(),
            limitPriceString.c_str(),
            stopPriceString.c_str(),
            order.order_type.ToChar(),
            order.order_restrict.ToChar(),
            order.order_qty,
            order.wrk_qty,
            executedQuantity,
            order.exec_qty,
            order.fill_qty,
            order.m_exchangeClearingAccount,
            order.GetTTMember(),
            order.GetTTGroup(),
            order.GetTTTrader(),
            order.GetExchangeMember(),
            order.GetExchangeGroup(),
            order.GetExchangeTrader(),
            MAXIMUM_ROUTING_KEY_OUTPUT, order.GetRoutingKey().c_str(),
            order.GetBrokerID(),
            order.GetCompanyID(),
            order.user_name.c_str(),
            order.acct.Enum2AuxStr().c_str(),
            FormatDate( order.order_exp_date ).c_str(),
            FormatTime( order.time_exch ).c_str(),
            orderNoStr,
            inet_ntoa( *( ( struct in_addr* )( &order.sender ) ) ),
            order.exchange_order_id,
            order.site_order_key,
            order.disclosed_qty,
            order.min_qty,
            order.stop_trigger_qty,
            order.order_flags,
            order.status_history );
        return buffer;
    }

    string TCFormatFill( const Fill& fill )
    {
        char buffer[ 1024 ];
        size_t bufferSize = sizeof( buffer );

#ifdef _DEBUG
        if(  !IsValidExchangeId( fill.srs.prod.srs_exch_id ))
        {
            TT_ASSERT( "FormatFill: invalid ExchangeId" );
        }
#endif 

        char fillOrderNoStr[64];
        SysTTUInt64toA(fill.order_no, fillOrderNoStr, 10);

        TT_SAFE_STATIC( defaultDate, Date );
        char clearingDateFormated[16] = { '\0' };
        if( defaultDate != fill.clearingDate )
        {
            static const char startOfClearingClearingDate[] = "CLR: ";
            BOOST_STATIC_ASSERT( sizeof( startOfClearingClearingDate ) < sizeof( clearingDateFormated ) );
            strncpy( clearingDateFormated, startOfClearingClearingDate, sizeof( startOfClearingClearingDate ) );
            fill.clearingDate.Format( clearingDateFormated + sizeof( startOfClearingClearingDate ) - 1, '/' );
        }

        SNPRINTF( buffer, bufferSize, 
                "%c,%u,%s,%u,%09d,%c,%c,%d,%7.*lf,%s"
                ",%12.12s,%12.12s,%12.12s,"
                "%s%s%s,%s%s%s,%.*s,%d,%d,%s,%s,%s,%s,%s,%.*s,%s,%s",
                ( fill.m_isIncomplete ? 'I' : 'C' ), fill.GetFillType().ToInt(), fill.fillKey, fill.GetVersion(),
                fill.record_no,
                fill.buy_sell.ToChar(),
                fill.open_close.ToChar(),
                abs( fill.long_qty ) + abs( fill.short_qty ),
                fill.srs.decimals,
                ( ( double )fill.match_prc )/( ( double )fill.srs.precision ),
                FormatSeries( fill.srs ).c_str(),
                fill.m_exchangeClearingAccount,
                fill.m_exchangeSubAccount, 
                fill.m_freeText,
                fill.GetTTMember(),
                fill.GetTTGroup(),
                fill.GetTTTrader(),
                fill.GetExchangeMember(),
                fill.GetExchangeGroup(),
                fill.GetExchangeTrader(),
                MAXIMUM_ROUTING_KEY_OUTPUT, fill.GetRoutingKey().c_str(),
                fill.GetBrokerID(),
                fill.GetCompanyID(),
                fill.user_name.c_str(),
                FormatDate( fill.trans_date ).c_str(),
                FormatTime( fill.trans_time ).c_str(),
                fillOrderNoStr,
                fill.exchange_order_id,
                MAXIMUM_TRANSACTION_IDENTIFIER_OUTPUT, fill.m_transactionIdentifier.c_str(),
                fill.site_order_key,
                clearingDateFormated );

        return buffer;
    }
} // anonymous

namespace transactioncounting_ns
{
    Persistance::Persistance()
        : m_bIsOpen(false)
          ,m_bReadOnly(false)
    {
    }

    Persistance::~Persistance()
    {
        Close();
        return;
    }

    bool Persistance::Open(const std::string& directory, bool bReadOnly)
    {
        m_databaseDirectory = directory;
        m_bReadOnly = bReadOnly;
        if (_access(m_databaseDirectory.c_str(), 0) < 0)
        {
            if (m_bReadOnly)
            {
                tt_log::GetCustomLog().Log( tt_log::SV_ERROR, LOGID, "[TransactionCounting] Database is read only and %s does not exist and will not be created.",  directory.c_str() );
                return false;
            }
            if (_mkdir(m_databaseDirectory.c_str()) < 0)
            {
                tt_log::GetCustomLog().Log( tt_log::SV_ERROR, LOGID, "[TransactionCounting] Cannot create %s error: %s.",  directory.c_str(), _sys_errlist[errno] );
                return false;
            }
        }

        string file(m_databaseDirectory);
        file += "/currentsession.dat";
        if (!m_currentSession.Open(file, bReadOnly, "CurrentSession"))
        {
            return false;
        }

        file = m_databaseDirectory;
        file += "/ordersandfills.dat";
        if (!m_ordersAndFills.Open(file, bReadOnly, "OrdersAndFills"))
        {
            m_ordersAndFills.Close();
            return false;
        }
        m_bIsOpen = true;
        return true;
    }

    void Persistance::Close()
    {
        m_currentSession.Close();
        m_ordersAndFills.Close();
        m_bIsOpen = false;
    }

    bool Persistance::AddOrder(const tt_core_ns::Order& order)
    {
        try
        {
            // Write the order
            string s = TCFormatOrder(order);
            StoragePosition pos;
            m_ordersAndFills.AddRecord( s.c_str(), s.size(), RECORD_ORDER, pos);

            return true;
        }
        catch (std::exception e)
        {
            tt_log::GetCustomLog().Log( tt_log::SV_ERROR, LOGID, "[TransactionCounting] %s",  e.what() );
            return false;
        }
    }

    bool Persistance::AddFill(const tt_core_ns::Fill& fill)
    {
        try
        {
            // Write the fill
            string s = TCFormatFill(fill);
            StoragePosition pos;
            m_ordersAndFills.AddRecord( s.c_str(), s.size(), RECORD_FILL, pos);

            return true;
        }
        catch (std::exception e)
        {
            tt_log::GetCustomLog().Log( tt_log::SV_ERROR, LOGID, "[TransactionCounting] %s",  e.what() );
            return false;
        }
    }

    bool Persistance::AddRecord(ExchangeCounts& ec, StoragePosition& pos)
    {
        try
        {
            m_currentSession.AddRecord(&ec, sizeof(ec), RECORD_EXCHANGE, pos);
            return true;
        }
        catch (const std::exception& e)
        {
            tt_log::GetCustomLog().Log( tt_log::SV_ERROR, LOGID, "[TransactionCounting] %s",  e.what() );
            return false;
        }
    }

    bool Persistance::UpdateRecord(const ExchangeCounts& rec)
    {
        try
        {
            TT_ASSERT(m_bReadOnly == false);
            StoragePosition pos = m_currentSession.Begin(rec.offset);
            TT_ASSERT(pos.RecordId() == RECORD_EXCHANGE);
            m_currentSession.UpdateRecord(reinterpret_cast<const void *>(&rec), sizeof(rec), pos);
            return true;
        }
        catch (const std::exception& e)
        {
            tt_log::GetCustomLog().Log( tt_log::SV_ERROR, LOGID, "[TransactionCounting] %s",  e.what() );
            return false;
        }
    }

    bool Persistance::AddRecord(UserCounts& uc, StoragePosition& pos)
    {
        try
        {
            m_currentSession.AddRecord(&uc, sizeof(uc), RECORD_USERNAME, pos);
            return true;
        }
        catch (const std::exception& e)
        {
            tt_log::GetCustomLog().Log( tt_log::SV_ERROR, LOGID, "[TransactionCounting] AddRecord %s",  e.what() );
            return false;
        }
    }

    bool Persistance::UpdateRecord(const UserCounts& rec)
    {
        try
        {
            TT_ASSERT(m_bReadOnly == false);
            StoragePosition pos = m_currentSession.Begin(rec.offset);
            TT_ASSERT(pos.RecordId() == RECORD_USERNAME);
            m_currentSession.UpdateRecord(reinterpret_cast<const void *>(&rec), sizeof(rec), pos);
            return true;
        }
        catch (const std::exception& e)
        {
            tt_log::GetCustomLog().Log( tt_log::SV_ERROR, LOGID, "[TransactionCounting] UpdateRecord %s",  e.what() );
            return false;
        }
    }

    StoragePosition Persistance::CurrentSessionRecordsBegin()
    {
        return m_currentSession.Begin();
    }

    bool Persistance::NextCurrentSessionRecord(StoragePosition& pos)
    {
        return m_currentSession.Next(pos);
    }

    StoragePosition Persistance::OrderAndFillRecordsBegin()
    {
        return m_ordersAndFills.Begin();
    }

    bool Persistance::NextOrderAndFillRecord(StoragePosition& pos)
    {
        return m_ordersAndFills.Next(pos);
    }

    bool Persistance::ReadRecord(ExchangeCounts& ec, const StoragePosition& pos)
    {
        try
        {
            TT_ASSERT(pos.RecordId() == RECORD_EXCHANGE);
            m_currentSession.ReadRecord(&ec, sizeof(ec), pos);
            return true;
        }
        catch (std::exception e)
        {
            tt_log::GetCustomLog().Log( tt_log::SV_ERROR, LOGID, "[TransactionCounting] %s",  e.what() );
            return false;
        }
    }

    bool Persistance::ReadRecord(UserCounts& uc, const StoragePosition& pos)
    {
        try
        {
            TT_ASSERT(pos.RecordId() == RECORD_USERNAME);
            m_currentSession.ReadRecord(&uc, sizeof(uc), pos);
            return true;
        }
        catch (std::exception e)
        {
            tt_log::GetCustomLog().Log( tt_log::SV_ERROR, LOGID, "[TransactionCounting] %s",  e.what() );
            return false;
        }
    }

    bool Persistance::GetOrderCsv(const StoragePosition& pos, std::string& s)
    {
        try
        {
            TT_ASSERT(pos.RecordId() == RECORD_ORDER);
            char *data = new char [pos.Size()+1];
            m_ordersAndFills.ReadRecord(data, pos.Size(), pos);
            data[pos.Size()] = '\0';
            s = data;
            delete data;
            return true;
        }
        catch (std::exception e)
        {
            tt_log::GetCustomLog().Log( tt_log::SV_ERROR, LOGID, "[TransactionCounting] %s",  e.what() );
            return false;
        }
    }

    bool Persistance::GetFillCsv(const StoragePosition& pos, std::string& s)
    {
        try
        {
            TT_ASSERT(pos.RecordId() == RECORD_FILL);
            char *data = new char [pos.Size()+1];
            m_ordersAndFills.ReadRecord(data, pos.Size(), pos);
            data[pos.Size()] = '\0';
            s = data;
            delete data;
            return true;
        }
        catch (std::exception e)
        {
            tt_log::GetCustomLog().Log( tt_log::SV_ERROR, LOGID, "[TransactionCounting] %s",  e.what() );
            return false;
        }
    }
}


