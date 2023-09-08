# * Save transformed data into a Snowflake table

# When you’re ready, just press Run to execute all the code at once. Make sure to check out the Results, Outputs, and Chart views to see the final tables produced from the code. 

# Import Snowpark for Python library and the DataFrame functions we will use in this Worksheet
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import month,year,col,sum

def main(session: snowpark.Session): 
    ### What is a Snowpark DataFrame
    # It represents a lazily-evaluated relational dataset that contains a collection of Row objects with columns defined by a schema (column name and type). 
    
    ### Load Data from Snowflake tables into Snowpark DataFrames
    # Let's load the campaign spend and revenue data we will use in this demo. This campaign spend table contains ad click data with aggregate daily spend across various digital ad channels. The revenue table contains revenue data for 10yrs. The data is available for preview under Snowpark_Demo_Schemas »Tables » Campaign_Spend and Monthly_Revenue. 

    snow_df_spend = session.table('campaign_spend')
    snow_df_revenue = session.table('monthly_revenue')

    ### Total Spend per Year and Month For All Channels
    # Let's transform the campaign spend data so we can see total cost per year/month per channel using _group_by()_ and _agg()_ Snowpark DataFrame functions.
    # TIP: For a full list of functions, see https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/_autosummary/snowflake.snowpark.functions.html#module-snowflake.snowpark.functions

    snow_df_spend_per_channel = snow_df_spend.group_by(year('DATE'), month('DATE'),'CHANNEL').agg(sum('TOTAL_COST').as_('TOTAL_COST')).\
    with_column_renamed('"YEAR(DATE)"',"YEAR").with_column_renamed('"MONTH(DATE)"',"MONTH").sort('YEAR','MONTH')

    # See the output of “print()” and “show()” in the "Output" tab below
    print("Total Spend per Year and Month For All Channels")
    snow_df_spend_per_channel.show()

    ### Total Spend Across All Channels
    # Let's further transform the campaign spend data so that each row represents total cost across all channels per year/month using the pivot() and sum() Snowpark DataFrame functions. 
    # This transformation lets us join with the revenue table so that our input features and target variable will be in a single table for model training.

    snow_df_spend_per_month = snow_df_spend_per_channel.pivot('CHANNEL',['search_engine','social_media','video','email']).sum('TOTAL_COST').sort('YEAR','MONTH')
    snow_df_spend_per_month = snow_df_spend_per_month.select(
        col("YEAR"),
        col("MONTH"),
        col("'search_engine'").as_("SEARCH_ENGINE"),
        col("'social_media'").as_("SOCIAL_MEDIA"),
        col("'video'").as_("VIDEO"),
        col("'email'").as_("EMAIL")
    )

    # See the output of “print()” and “show()” in the "Output" tab below
    print("Total Spend Across All Channels")
    snow_df_spend_per_month.show()

    ### Total Revenue per Year and Month
    # Now let's transform the revenue data into revenue per year/month using group_by() and agg() functions.

    snow_df_revenue_per_month = snow_df_revenue.group_by('YEAR','MONTH').agg(sum('REVENUE')).sort('YEAR','MONTH').with_column_renamed('SUM(REVENUE)','REVENUE')

    # See the output of “print()” and “show()” in the "Output" tab below
    print("Total Revenue per Year and Month")
    snow_df_revenue_per_month.show()

    ### Join Total Spend and Total Revenue per Year and Month Across All Channels
    # Next let's join this revenue data with the transformed campaign spend data so that our input features (i.e. cost per channel) and target variable (i.e. revenue) can be loaded into a single table for model training. 
    snow_df_spend_and_revenue_per_month = snow_df_spend_per_month.join(snow_df_revenue_per_month, ["YEAR","MONTH"])

    # See the output of “print()” and “show()” in the "Output" tab below
    print("Total Spend and Revenue per Year and Month Across All Channels")
    snow_df_spend_and_revenue_per_month.show()

    ### Examine Query Explain Plan
    # Snowpark makes it really convenient to look at the DataFrame query and execution plan using explain() Snowpark DataFrame function.
    # See the output of “explain()” in the "Output" tab below
    snow_df_spend_and_revenue_per_month.explain()

    ### Save Transformed Data into Snowflake Table
    # Let's save the transformed data into a Snowflake table *SPEND_AND_REVENUE_PER_MONTH*

    snow_df_spend_and_revenue_per_month.write.mode('overwrite').save_as_table('SPEND_AND_REVENUE_PER_MONTH')

    # See the output of this in "Results" tab below
    return snow_df_spend_and_revenue_per_month

 ### Continue Your Journey
    # Hop over to the Snowpark page for more learning on data pipelines: https://www.snowflake.com/en/data-cloud/snowpark/

    # It’s important to note that you are also able to connect to Snowflake with Snowpark from your favorite notebook or IDE. Learn more about setting up your development environment in Snowflake Documentation: https://docs.snowflake.com/en/developer-guide/snowpark/python/setup 

    # Snowpark is pre-installed with thousands of open-source packages from the Anaconda repository plus the Conda package manager for dependency management without any extra cost. The full list of packages can be found here: https://repo.anaconda.com/pkgs/snowflake/

    # To go into more advanced demos, you can integrate open-source Python libraries curated by Anaconda at no additional charges beyond warehouse usage. All you need to do is accept the usage terms today. To do this, make sure you are in the ORGADMIN role in your account then click Admin » Billing » Terms & Billing. Scroll to the Anaconda section and click the Enable button. More details in documentation: https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages

    # If you’d like a more guided experience, supplemental video and screenshots go to:: https://quickstarts.snowflake.com/guide/getting_started_with_snowpark_in_snowflake_python_worksheets/index.html
#####
