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
 * $Source: persistance.h $
 * $Date: 2012/04/26 10:31:29CDT $
 * $Author: Jeff Richards (TT) (jrichards) $
 * $Revision: 1.1 $
 ***************************************************************************/
 
#pragma once

#ifndef TRANSACTION_PERSISTANCE_H
#define TRANSACTION_PERSISTANCE_H

#include "boost/noncopyable.hpp"
#include "StorageFile.h"

namespace tt_core_ns
{
    class Order;
    class Fill;
}

namespace transactioncounting_ns
{

    struct ExchangeCounts;
    struct UserCounts;

    // OrderCsv cnd FillCsv column info
    const char *const orderColumns = "order_status,isIncomplete,order_action,buy_sell,open_close,series,limit_prc,stop_prc,"
            "order_type,order_restrict,order_qty,wrk_qty,executedQuantity,exec_qty,fill_qty,exchangeClearingAccount,TraderMGT,"
            "ExchangeMGT,RoutingKey,BrokerID,CompanyID,user_name,acct,order_exp_date,time_exch,orderNoStr,sender IP,exchange_order_id,"
            "site_order_key,disclosed_qty,min_qty,stop_trigger_qty,order_flags,status_history";

    const char *const fillColumns = "isIncomplete,filltype,fillKey,Version,record_no,buy_sell,open_close,position,match_prc,Series,exchangeClearingAccount,"
            "exchangeSubAccount,m_freeText,TraderMGT,ExchangeMGT,RoutingKey,BrokerID,CompanyID,user_name,trans_date,trans_time,fillOrderNoStr,exchange_order_id,"
            "transactionIdentifier,site_order_key,clearingDateFormated";

    class Persistance
    {
    public:
        Persistance();
        ~Persistance();
        bool Open(const std::string& directory, bool readOnly);
        void Close();
    
        bool isopen() {return m_bIsOpen;}
        bool AddOrder(const tt_core_ns::Order& order);
        bool AddFill(const tt_core_ns::Fill& fill);
        bool AddRecord(ExchangeCounts& ec, StoragePosition& pos);
        bool AddRecord(UserCounts& uc, StoragePosition& pos);
        bool UpdateRecord(const ExchangeCounts& rec);
        bool UpdateRecord(const UserCounts& rec);
        bool ReadRecord(UserCounts& uc, const StoragePosition& pos);
        bool ReadRecord(ExchangeCounts& ec, const StoragePosition& pos);

        // Iterate through the counts of the current session counts
        StoragePosition CurrentSessionRecordsBegin();
        bool NextCurrentSessionRecord(StoragePosition& pos);

        // Iterate through the orders and fills
        StoragePosition OrderAndFillRecordsBegin();
        bool NextOrderAndFillRecord(StoragePosition& pos);

        bool GetExchangeCount(const StoragePosition& pos, ExchangeCounts&);
        bool GetUserCount(const StoragePosition& pos, UserCounts&);

        // Get a CVS delimited order or fill
        bool GetOrderCsv(const StoragePosition& pos, std::string&);
        bool GetFillCsv(const StoragePosition& pos, std::string&);
    private:
        bool m_bIsOpen;
        StorageFile m_currentSession;
        StorageFile m_ordersAndFills;
        bool m_bReadOnly;
        std::string m_databaseDirectory;
    };
}

#endif // TRANSACTION_PERSISTANCE_H

