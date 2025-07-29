from dagster import asset, AssetExecutionContext
import pandas as pd
from datetime import datetime, date
from .scraper import scrape_exchange_rates, clean_exchange_rates
from .resources import ClickHouseResource 

@asset
def raw_exchange_rates(context: AssetExecutionContext):
    """Scrape raw exchange rates from BNR Rwanda website"""
    context.log.info("Starting raw_exchange_rates asset")
    df, update_time = scrape_exchange_rates()
    
    if df is not None:
        context.log.info(f"Successfully scraped {len(df)} records")
        context.log.info(f"DataFrame head:\n{df.head()}")
        context.log.info(f"DataFrame columns: {df.columns.tolist()}")
        return {"data": df, "update_time": update_time}
    
    context.log.error("Scraping returned None")
    return None

@asset
def processed_exchange_rates(
    context: AssetExecutionContext,
    raw_exchange_rates,
    clickhouse: ClickHouseResource
):
    """Process exchange rates, handling missing or unchanged data"""
    context.log.info("Starting processed_exchange_rates asset")
    
    today = date.today()
    
    if raw_exchange_rates is None:
        context.log.info("No new rates found - fetching most recent rates from ClickHouse")
        
        # Query to get the most recent rates
        query = """
        SELECT 
            currency,
            buying_rate,
            average_rate,
            selling_rate,
            rate_date,
            source_update_time
        FROM daily_exchange_rates
        WHERE rate_date = (
            SELECT MAX(rate_date)
            FROM daily_exchange_rates
            WHERE is_actual_rate = true
        )
        """
        
        try:
            most_recent_rates = clickhouse.query_df(query)
            if most_recent_rates is not None and not most_recent_rates.empty:
                context.log.info(f"Found {len(most_recent_rates)} previous rates")
                
                # Update the metadata for today's entry
                most_recent_rates['rate_date'] = today
                most_recent_rates['is_actual_rate'] = False
                most_recent_rates['source_update_time'] = datetime.now()
                
                return most_recent_rates
            else:
                context.log.error("No previous rates found in ClickHouse")
                return None
                
        except Exception as e:
            context.log.error(f"Error fetching previous rates: {str(e)}")
            return None

    # Process new rates normally
    context.log.info("Processing new rates")
    current_data = raw_exchange_rates["data"]
    current_update_time = raw_exchange_rates["update_time"]
    
    if current_data is not None:
        cleaned_df = clean_exchange_rates(current_data)
        if cleaned_df is not None:
            context.log.info(f"Cleaned data shape: {cleaned_df.shape}")
            
            # Add metadata columns
            cleaned_df['rate_date'] = today
            cleaned_df['is_actual_rate'] = True
            cleaned_df['source_update_time'] = current_update_time
            return cleaned_df
        
        context.log.error("Cleaning returned None")
    
    context.log.error("No data available")
    return None

@asset
def exchange_rates_clickhouse(
    context: AssetExecutionContext,
    processed_exchange_rates,
    clickhouse: ClickHouseResource
):
    """Load processed exchange rates into ClickHouse, preventing duplicates"""
    context.log.info("Starting exchange_rates_clickhouse asset")
    
    if processed_exchange_rates is None:
        context.log.error("processed_exchange_rates is None")
        return None

    try:
        context.log.info(f"Data to be loaded shape: {processed_exchange_rates.shape}")
        context.log.info(f"Data to be loaded head:\n{processed_exchange_rates.head()}")
        
        # Get unique combinations of currency and date from new data
        unique_df = processed_exchange_rates.drop_duplicates(
            subset=['currency', 'rate_date'], 
            keep='last'
        )
        
        # Since we're using ReplacingMergeTree, we can directly insert the data
        # ClickHouse will handle deduplication based on the ORDER BY columns (rate_date, currency)
        context.log.info(f"Loading {len(unique_df)} records into ClickHouse")
        clickhouse.load_dataframe(unique_df)
        
        context.log.info(
            f"Loaded {len(unique_df)} records into ClickHouse. "
            f"Actual rates: {unique_df['is_actual_rate'].sum()}, "
            f"Carried-over rates: {(~unique_df['is_actual_rate']).sum()}"
        )
        # Update monthly exchange rates immediately for the current month
        context.log.info("Updating monthly exchange rates for the current month")
        try:
            # First delete existing records for the current month
            delete_query = """
            DELETE FROM irembopay_analytics.monthly_exchange_rates
            WHERE month = toLastDayOfMonth(today())
            """
            clickhouse.execute(delete_query)
            
            # Then insert new records from daily_exchange_rates
            insert_query = """
            INSERT INTO irembopay_analytics.monthly_exchange_rates (currency_code, average_rate, month)
            SELECT 
                currency AS currency_code, 
                average_rate, 
                toLastDayOfMonth(rate_date) AS month 
            FROM 
                irembopay_analytics.daily_exchange_rates 
            WHERE 
                rate_date = today()
            """
            clickhouse.execute(insert_query)
            
            # Verify the update
            verify_query = """
            SELECT count(*) as record_count 
            FROM irembopay_analytics.monthly_exchange_rates 
            WHERE month = toLastDayOfMonth(today())
            """
            
            result = clickhouse.query_df(verify_query)
            monthly_record_count = result['record_count'].iloc[0] if not result.empty else 0
            
            context.log.info(f"Updated monthly exchange rates for current month. Found {monthly_record_count} records.")
            
        except Exception as e:
            context.log.error(f"Failed to update monthly exchange rates: {str(e)}")
            # Don't raise the exception here to avoid failing the entire asset
            # Just log the error and continue

        return len(unique_df)
            
    except Exception as e:
        context.log.error(f"Failed to load data into ClickHouse: {str(e)}")
        context.log.error(f"Exception details: {str(e)}")
        raise
@asset
def refresh_monthly_exchange_rates(
    context: AssetExecutionContext,
    clickhouse: ClickHouseResource
):
    """Refresh the monthly exchange rates table  to add previous month's data"""
    context.log.info("Starting refresh_monthly_exchange_rates asset")
    
    today = date.today()
    # Check if today is the first day of the month
    is_first_day_of_month = today.day == 1
    
    if is_first_day_of_month:
        context.log.info("Today is the first day of the month. Inserting monthly exchange rates.")
        try:
            # First, verify if there's data for the previous month
            prev_month_start = (today.replace(day=1) - datetime.timedelta(days=1)).replace(day=1)
            prev_month_end = today.replace(day=1) - datetime.timedelta(days=1)
            
            verify_data_query = f"""
            SELECT count(*) as record_count 
            FROM irembopay_analytics.daily_exchange_rates 
            WHERE rate_date BETWEEN '{prev_month_start}' AND '{prev_month_end}'
            """
            
            result = clickhouse.query_df(verify_data_query)
            daily_record_count = result['record_count'].iloc[0] if not result.empty else 0
            
            if daily_record_count == 0:
                context.log.warning(f"No daily exchange rate records found for {prev_month_start} to {prev_month_end}")
                return {"refresh_date": today, "refreshed": False, "reason": "No data for previous month"}

            # First delete any existing data for the previous month
            delete_query = f"""
            DELETE FROM irembopay_analytics.monthly_exchange_rates
            WHERE month = '{prev_month_end}'
            """
            clickhouse.execute(delete_query)
            context.log.info(f"Deleted existing records for month ending {prev_month_end}")
            
            # Execute the REFRESH command to insert previous month rates
            refresh_query = f"""
            INSERT INTO irembopay_analytics.monthly_exchange_rates(currency_code,average_rate,`month` ) 
            SELECT
            currency AS currency_code,
            avg(selling_rate) AS average_rate,
            toLastDayOfMonth(rate_date) AS month
            FROM irembopay_analytics.daily_exchange_rates
            WHERE toStartOfMonth(rate_date) = addMonths(toStartOfMonth(today()), -1)
            GROUP BY currency,toLastDayOfMonth(rate_date)
            ORDER BY currency_code;
            """
            clickhouse.execute(refresh_query)
            
            # Verify that the previous month's data was loaded
            verify_query = f"""
            SELECT count(*) as record_count 
            FROM irembopay_analytics.monthly_exchange_rates 
            WHERE month = '{prev_month_end}'
            """
            
            result = clickhouse.query_df(verify_query)
            monthly_record_count = result['record_count'].iloc[0] if not result.empty else 0
            
            context.log.info(f"Inserted monthly exchange rates. Found {monthly_record_count} records for {prev_month_end}.")
            return {
                "refresh_date": today, 
                "refreshed": True, 
                "record_count": monthly_record_count,
                "month_end": prev_month_end
            }
        
        except Exception as e:
            context.log.error(f"Failed to insert monthly exchange rates: {str(e)}")
            raise
    else:
        context.log.info("Not the first day of the month. Skipping monthly exchange rates insert.")
        return {"refresh_date": today, "refreshed": False, "reason": "Not first day of month"}