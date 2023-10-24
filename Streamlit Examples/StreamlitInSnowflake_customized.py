# Snowpark for Python API reference: https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/index.html
# Snowpark for Python Developer Guide: https://docs.snowflake.com/en/developer-guide/snowpark/python/index.html
# Streamlit docs: https://docs.streamlit.io/
import json
import calendar
import altair as alt
import pandas as pd
from snowflake.snowpark.session import Session
from snowflake.snowpark import Table
import snowflake.snowpark.functions as F
import streamlit as st
from snowflake.snowpark.context import get_active_session

INITS="re"
SEASON='fall'
st.title("Lennar Ad Spend Optimizer")
session = get_active_session()

#Create a SQL function to pass the slider values to the predict_leads function
def get_new_predicted_leads(session: Session, new_allocations: dict, season: str) -> float:
    return (
        session
            .sql(
                f"""SELECT predict_leads_{INITS}(array_construct(
                    '{season}',
                    {new_allocations['OFFLINE MARKETING']},
                    {new_allocations['3RD PARTY LISTING']},
                    {new_allocations['EMAIL']},
                    {new_allocations['SOCIAL']},
                    {new_allocations['OTT']},
                    {new_allocations['DIGITAL NOT SET']},
                    {new_allocations['RADIO']},
                    {new_allocations['SPEND FORM']},
                    {new_allocations['DISPLAY']},
                    {new_allocations['SEM']}
                ))"""
            )
            .to_pandas()
            .iloc[0, 0]
    )


# Display advertising budget sliders and set their default values
st.header("Advertising budgets")
st.caption("Use the sliders below to adjust how much budget to allocate to each channel. Click 'Predict Leads' to predict how many leads will be generated")

# Build out sliders for each channel spend amount
with st.form("form_budget_allocation"):
    col1, _, col2 = st.columns([4, 1, 4])
    new_allocations = {"OFFLINE MARKETING":0,"3RD PARTY LISTING":0,"EMAIL":0,"SOCIAL":0, "OTT":0,"DIGITAL NOT SET":0,"RADIO":0,"SPEND FORM":0,"DISPLAY":0,"SEM":0}
  # Create a list of columns to cycle through
    columns = [col1, col2] * (len(new_allocations) // 2)
    
    for channel, col in zip(new_allocations.keys(), columns):
        with col:
            budget = st.slider(channel, 0, 3000000, 1000)
            new_allocations[channel] = budget

            
    with col1:
        submit = st.form_submit_button("Predict leads")

if submit == True:
    
    predicted_leads=get_new_predicted_leads(
        session=session, 
        new_allocations=new_allocations, 
        season=SEASON
    )


    df_predicted = pd.DataFrame({
        'YEAR': [2023] * len(new_allocations),  # Adding month as "10" for all rows
        'MONTH': [10] * len(new_allocations),  # Adding month as "10" for all rows
        'SEASON': [SEASON] * len(new_allocations),  # Using the SEASON value for all rows
        'LEADS': [predicted_leads] * len(new_allocations),  # assuming predicted_leads is a single value for all channels
        'CHANNEL': list(new_allocations.keys()),
        'BUDGET': list(new_allocations.values())
        
    })


    #Data preparation, need to unpivot the data to get it to a way that the charting library will work:
    
    snow_df1 = session.table(f"MARKETING_BUDGETS_LEADS_{INITS}").filter(
        (F.col("YEAR") == 2023) & 
        (F.col("MONTH") <=9)
    )
    snow_df2 = snow_df1.unpivot("Budget", "Channel", ["OFFLINE MARKETING","3RD PARTY LISTING","EMAIL","SOCIAL", "OTT","DIGITAL NOT SET","RADIO", "SPEND FORM","DISPLAY","SEM"])

    df1 = snow_df1.to_pandas()
    last_month_leads=df1["LEADS"].iloc[-1]

    pct_change = 100*(predicted_leads - last_month_leads) / last_month_leads
    st.markdown("---")
    st.metric("", f"Predicted leads {predicted_leads:,.0f} ", f"{pct_change:.1f} % vs last month")

    df=snow_df2.to_pandas()
  #  st.dataframe(df_predicted)
  #  st.dataframe(df)

    result_df=pd.concat([df, df_predicted], ignore_index=True)
   # st.dataframe(result_df)
    
    # Create the stacked bar chart for BUDGET
    bars = alt.Chart(result_df).mark_bar().encode(
        x='MONTH:O',
        y=alt.Y('BUDGET:Q', stack='zero', title='Budget'),
        color='CHANNEL:N',
        tooltip=['CHANNEL', 'BUDGET', 'LEADS']
    )
    
    # Create the line chart for LEADS
    leads = result_df.groupby('MONTH').agg({'LEADS':'first'}).reset_index()  # get the unique LEADS value for each month
    line = alt.Chart(result_df).mark_line(color='black', point=True).encode(
        x='MONTH:O',
        y=alt.Y('LEADS:Q', title='Leads', axis=alt.Axis(orient='right')),
        tooltip='LEADS'
    )
    
    # Combine the bar chart and the line chart
    chart = alt.layer(bars, line).resolve_scale(
        y='independent'  # this ensures that the Y scales for bars and line are independent
    )
    st.altair_chart(chart, use_container_width=True)
    
    
    
## OPTIONAL-- ENHANCED BAR CHART IF YOU WANT MORE FORMATTING OPTIONS  


    # Find the last month in the dataframe
    last_month = result_df['MONTH'].max()


    # ENHANCED BAR CHART OPTIONAL Create the stacked bar chart for BUDGET with a new color scheme and opacity
    bars = alt.Chart(result_df).mark_bar(opacity=0.7).encode(
        x='MONTH:O',
        y=alt.Y('BUDGET:Q', stack='zero', title='Budget'),
        color=alt.Color('CHANNEL:N', scale=alt.Scale(scheme='tableau20')),  # Using the 'tableau20' color scheme
        stroke=alt.condition(
            alt.datum.MONTH == last_month,
            alt.value('black'),  # Stroke color for the last month
            alt.value(None)  # No stroke for other months
        ),
        strokeWidth=alt.condition(
            alt.datum.MONTH == last_month,
            alt.value(2),  # Stroke width for the last month
            alt.value(0)  # No stroke width for other months
        ),
        strokeDash=alt.condition(
            alt.datum.MONTH == last_month,
            alt.value([5, 5]),  # Dotted pattern for the last month
            alt.value([0, 0])  # No pattern for other months
        ),
        tooltip=['CHANNEL', 'BUDGET', 'LEADS'],
        opacity=alt.condition("datum.BUDGET > 0", alt.value(0.7), alt.value(0.1))  # Enhance opacity for non-zero values
    ).interactive()
    
    # Create the line chart for LEADS with modified hover capabilities
    leads = result_df.groupby('MONTH').agg({'LEADS':'first'}).reset_index()  # get the unique LEADS value for each month
    line = alt.Chart(leads).mark_line(point=True).encode(
        x='MONTH:O',
        y=alt.Y('LEADS:Q', title='Leads', axis=alt.Axis(orient='right')),
        tooltip=alt.Tooltip('LEADS:Q', title='Leads'),  # Display "Leads" in the tooltip
        size=alt.condition(~alt.datum.LEADS, alt.value(1), alt.value(3))  # Enhance line width on hover
    ).interactive()
    
    # Combine the bar chart and the line chart
    chart = alt.layer(bars, line).resolve_scale(
        y='independent'  # this ensures that the Y scales for bars and line are independent
    )
    st.altair_chart(chart, use_container_width=True)





