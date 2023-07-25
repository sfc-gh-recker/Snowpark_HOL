# Snowpark for Python API reference: https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/index.html
# Snowpark for Python Developer Guide: https://docs.snowflake.com/en/developer-guide/snowpark/python/index.html
# Streamlit docs: https://docs.streamlit.io/

import json
import altair as alt
import pandas as pd
from snowflake.snowpark.session import Session
from snowflake.snowpark import Table
import snowflake.snowpark.functions as F
import streamlit as st

INITS="re"
MONTH="2022-04-01"
SEASON="'spring'"
from snowflake.snowpark.context import get_active_session


def load_total_cost_data(session: Session, group_cols: list) -> Table:

    seasons=(
        F.when(F.col("MONTH").isin(12, 1, 2),"winter")
        .when(F.col("MONTH").isin(3, 4, 5),"spring")
        .when(F.col("MONTH").isin(6, 7, 8),"summer")
        .when(F.col("MONTH").isin(9, 10, 11),"fall")
    )

    return (
        session
            .table("CAMPAIGN_SPEND")
            .with_column("YEAR",F.year(F.col("DATE")))
            .with_column("MONTH",F.month(F.col("DATE")))
            .with_column("SEASON", seasons)
            .group_by(group_cols)
            .agg(F.sum(F.col("TOTAL_COST")).as_("TOTAL_COST"))
            # .sort(F.col("YEAR").asc(), F.col("MONTH").asc())
    )


def load_revenue_data(session: Session) -> Table:
    return session.table("MONTHLY_REVENUE")


def join_data(df_cost: Table, df_revenue: Table) -> pd.DataFrame:
    return (
        df_cost
            .join(df_revenue, on=["MONTH","YEAR"])
            .with_column("DATE", F.date_from_parts(F.col("YEAR"), F.col("MONTH"), 1))
            .filter((F.col("DATE")>="2021-04-01") & (F.col("DATE")<"2022-04-01"))
            .drop([F.col("YEAR"), F.col("MONTH")])
            .sort(F.col("DATE").asc())
            .to_pandas()
    )


def get_new_predicted_revenue(session: Session, new_allocations:dict, season=str) -> float:
        return (
            session
                .sql(f"SELECT predict_roi_re(array_construct({season},{new_allocations['search_engine']}, {new_allocations['social_media']}, {new_allocations['video']}, {new_allocations['email']}))")
                .to_pandas()
                .iloc[0,0]
        )

def add_new_prediction(df_hist: pd.DataFrame, new_allocations: dict, predicted_revenue: float, predicted_month=str) -> pd.DataFrame:

    df_allocations = pd.DataFrame.from_dict({"CHANNEL":new_allocations.keys(),"TOTAL_COST":new_allocations.values()})
    df_allocations["REVENUE"]=predicted_revenue
    df_allocations["SEASON"]="spring"
    df_allocations["DATE"]=predicted_month
    
    return pd.concat([df_hist,df_allocations])

# Streamlit config
st.set_page_config("Travel & Leisure Ad Spend Optimizer Ad Spend Optimizer", "centered")
#st.write("<style>[data-testid='stMetricLabel'] {min-height: 0.5rem !important}</style>", unsafe_allow_html=True)

st.title("SportsCo Ad Spend Optimizer")

# Call functions to get Snowflake session and load data
session = get_active_session()
sdf_total_cost=load_total_cost_data(session, group_cols=["MONTH","YEAR","SEASON","CHANNEL"])
sdf_revenue=load_revenue_data(session)
pdf_cost_and_revenue=join_data(sdf_total_cost, sdf_revenue)

# Display advertising budget sliders and set their default values
st.header("Advertising budgets")
st.caption("Use the sliders below to adjust how much budget to allocate to each channel. Click 'Predict revenue' to predict how much revenue will be generated")

with st.form("form_budget_allocation"):
    col1, _, col2 = st.columns([4, 1, 4])
    new_allocations = {"search_engine":0, "social_media":0, "email":0, "video":0}
    for channel, col in zip(new_allocations.keys(), [col1, col1, col2, col2]):
        with col:
            budget = st.slider(channel, 0, 1000000, 1000)
            new_allocations[channel] = budget
    with col1:
        submit=st.form_submit_button("Predict revenue")


if submit == True:

    predicted_revenue=get_new_predicted_revenue(
        session=session, 
        new_allocations=new_allocations, 
        season=SEASON
    )

    last_month_revenue=pdf_cost_and_revenue["REVENUE"].iloc[-1]

    pct_change = 100*(predicted_revenue - last_month_revenue) / last_month_revenue

    df_monthly_revenue = add_new_prediction(
        pdf_cost_and_revenue,
        new_allocations=new_allocations,
        predicted_revenue=predicted_revenue,
        predicted_month=MONTH
    )
    
    st.markdown("---")
    st.metric("", f"Predicted revenue ${predicted_revenue:,.0f} ", f"{pct_change:.1f} % vs last month")

    base = alt.Chart(df_monthly_revenue).encode(alt.X("DATE", title="Date", axis=alt.Axis(labelAngle=-45)))
    bars = base.mark_bar().encode(
        y=alt.Y("TOTAL_COST", title="Budget"),
        color=alt.Color("CHANNEL", legend=alt.Legend(orient="top", title=" ")),
        opacity=alt.condition(alt.datum.DATE == MONTH, alt.value(1), alt.value(0.4))
    )

    lines = base.mark_line(size=3).encode(
        y=alt.Y("max(REVENUE)", title="Revenue"), 
        color=alt.value("#808495"),
    )
    points = base.mark_point(strokeWidth=3).encode(
        y=alt.Y("REVENUE"),
        stroke=alt.value("#808495"),
        fill=alt.value("white"),
    )
    chart = alt.layer(bars, lines + points).resolve_scale(y="independent")
    chart = (
        chart
            .configure_view(strokeWidth=0)
            .configure_axisY(domain=False)
            .configure_axis(
                labelColor="#808495",
                tickColor="#e6eaf1",
                gridColor="#e6eaf1",
                domainColor="#e6eaf1",
                titleFontWeight=600,
                titlePadding=10,
                labelPadding=5,
                labelFontSize=14
            )
            .configure_range(category=["#FFE08E", "#03C0F2", "#FFAAAB", "#995EFF"])
    )
    st.altair_chart(chart, use_container_width=True)
