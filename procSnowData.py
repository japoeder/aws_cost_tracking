from utilities.mysqlConnect import *
from utilities.getCredentials import *
import pprint


def process_mysql(cf):
    #####################################
    #####   mysql - DATA LOAD  ######
    #####################################

    def create_sf_queries(region):
        usageQuery = f"""
            with use_history as
                (
                    select
                        USAGE_DATE
                        , DATABASE_ID
                        , DATABASE_NAME
                        , DELETED
                        , AVERAGE_DATABASE_BYTES
                        , AVERAGE_FAILSAFE_BYTES
                        , AVERAGE_DATABASE_BYTES / 1000000000000 as teras
                        , AVERAGE_FAILSAFE_BYTES/ 1000000000000 as teras_failsafe
                        , year(usage_date) as year
                        , month(usage_date) as month
        
                    from mysql.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY
                    where
                        year(usage_date) >= 2021
                        and database_id not in (
                                                select distinct database_id
                                                from mysql.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY
                                                where deleted is not null)
                )
                    , daily_costs as
                (
                    select
        
                        year(usage_date) as year
                        , month(usage_date) as month
                        , database_name
                        , avg(teras / day(last_day(usage_date))) as daily_teras
                        , avg(teras_failsafe / day(last_day(usage_date))) as daily_teras_failsafe
        
                    from use_history
                    group by
                        year(usage_date)
                        , month(usage_date)
                        , database_name
                )
                    select
        
                        '{region}' as region
                        , a.USAGE_DATE
                        , a.DATABASE_ID
                        , a.DATABASE_NAME
                        , a.DELETED
                        , a.AVERAGE_DATABASE_BYTES
                        , a.AVERAGE_FAILSAFE_BYTES
                        , a.teras
                        , a.teras_failsafe
                        , a.year
                        , a.month
                        , b.daily_teras
                        , b.daily_teras_failsafe
        
                    from use_history a
                    LEFT JOIN
                    daily_costs b
                    ON
                        a.year = b.year
                        and a.month = b.month
                        and a.database_name = b.database_name;
        """

        meterQuery = f"""
            select '{region}' as region, * 
            from mysql.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            where
               year(start_time) >= 2021;
        """

        return [usageQuery, meterQuery]

    # Grab credentials for AMZ (us-east)
    credList = get_cred(
        file=cf[0],
        env_user=cf[1]["sf_west_user"],
        env_pwd=cf[1]["sf_west_pwd"],
    )

    # Instantiate connection dictionary
    connDict = paramDefs(credList[0], credList[1])

    # Grab updated mysql data west
    queryBuild = create_sf_queries("west")
    usageQuery = queryBuild[0]
    meterQuery = queryBuild[1]

    payload = connDict.copy()
    payload["role"] = "accountadmin"
    payload["dl"] = True
    payload["account"] = "bn25767"

    payload["query"] = usageQuery
    usageData_west = snowQuery(payload)

    payload["query"] = meterQuery
    meterData_west = snowQuery(payload)

    # Grab credentials for AMZ (us-east)
    credList = get_cred(
        file=cf[0],
        env_user=cf[1]["sf_east_user"],
        env_pwd=cf[1]["sf_east_pwd"],
    )

    # Instantiate connection dictionary
    connDict = paramDefs(credList[0], credList[1])

    # Grab updated mysql data from east
    queryBuild = create_sf_queries("east")
    usageQuery = queryBuild[0]
    meterQuery = queryBuild[1]

    payload = connDict.copy()
    payload["role"] = "accountadmin"
    payload["dl"] = True

    payload["query"] = usageQuery
    usageData_east = snowQuery(payload)

    payload["query"] = meterQuery
    meterData_east = snowQuery(payload)

    # Clear out east tables used by tableau
    clearUsage = """
    delete from JPOEDER_AA.DW.DATABASE_STORAGE_USAGE_HISTORY;
    """
    clearMetering = """
    delete from JPOEDER_AA.DW.WAREHOUSE_METERING_HISTORY;
    """

    payload = connDict.copy()
    payload["role"] = "tech"

    payload["query"] = clearMetering
    snowQuery(payload)
    payload["query"] = clearUsage
    snowQuery(payload)

    # Load data from account usage schema
    payload = connDict.copy()
    payload["role"] = "tech"
    payload["write"] = True
    payload["auto_create_table"] = True
    payload["overwrite"] = True

    payload["uploadTargetTable"] = "DATABASE_STORAGE_USAGE_HISTORY"
    payload["df"] = usageData_east
    snowQuery(payload)
    payload["df"] = usageData_west
    snowQuery(payload)

    payload["uploadTargetTable"] = "WAREHOUSE_METERING_HISTORY"
    payload["df"] = meterData_east
    snowQuery(payload)
    payload["df"] = meterData_west
    snowQuery(payload)
