import os

import pandas as pd
import plotly.express as px
import streamlit as st

DATA_PATH = os.environ.get("DEPOT_MERGED_PATH", "./data/depot_merged.csv")


@st.cache_data
def load_data(path: str) -> pd.DataFrame:
    return pd.read_csv(path)


st.title("Interactive Dashboard Example")

try:
    df = load_data(DATA_PATH)
except FileNotFoundError:
    st.error(
        f"Data file not found at `{DATA_PATH}`. "
        "Run the pipeline first: `.venv/bin/python -m depot_risk_assessment.main`"
    )
    st.stop()

col1, col2, col3 = st.columns(3)

standort_options = sorted([x for x in df["Standort"].dropna().unique().tolist()])
with col1:
    selected_standort = st.multiselect("Select Standort:", options=standort_options, default=standort_options)

sektor_options = sorted([x for x in df["Sektor"].dropna().unique().tolist()])
with col2:
    selected_sektor = st.multiselect("Select Sektor:", options=sektor_options, default=sektor_options)

type_options = sorted([x for x in df["Type"].dropna().unique().tolist()])
with col3:
    selected_type = st.multiselect("Select Type:", options=type_options, default=type_options)

filtered_df = df[
    (df["Standort"].isin(selected_standort)) & (df["Sektor"].isin(selected_sektor)) & (df["Type"].isin(selected_type))
]

total_wert = filtered_df["Wert"].sum()
grouped_wert = filtered_df.groupby(["Name"]).agg({"Wert": "sum"})["Wert"].to_frame()
grouped_wert.reset_index(inplace=True)

st.metric(label="Total Wert", value=f"€{total_wert:,.2f}")

type_pie_chart = px.pie(
    filtered_df,
    names="Type",
    values="Wert",
    title="Distribution by Type",
    hole=0.5,
    hover_data={"Wert": ":.2f"},
)
type_pie_chart.update_traces(hovertemplate="%{label}: %{value:.2f} (%{percent:.2%})")
type_pie_chart.update_layout(hoverlabel=dict(font_size=20))
st.plotly_chart(type_pie_chart)

col1, col2 = st.columns(2)
with col1:
    display_mode = st.selectbox("Select display mode:", ["Wert_Percentage", "Wert"])
with col2:
    num_top_names = st.number_input(
        "Select number of top names to display:",
        min_value=1,
        max_value=50,
        value=10,
        step=1,
    )

grouped_wert["Wert_Percentage"] = grouped_wert["Wert"] / total_wert * 100 if total_wert > 0 else 0
top_names_df = grouped_wert.nlargest(num_top_names, display_mode)
bar_chart = px.bar(
    top_names_df,
    x="Name",
    y=display_mode,
    title=f"Top {num_top_names} Names by {display_mode}",
)
bar_chart.update_layout(hoverlabel=dict(font_size=16))
st.plotly_chart(bar_chart)

col1, col2 = st.columns(2)

with col1:
    sector_pie_chart = px.pie(
        filtered_df,
        names="Sektor",
        values="Wert",
        title="Distribution by Sektor",
        hover_data={"Wert": ":.2f"},
    )
    sector_pie_chart.update_traces(hovertemplate="%{label}: %{value:.2f} (%{percent:.2%})")
    sector_pie_chart.update_layout(hoverlabel=dict(font_size=16))
    st.plotly_chart(sector_pie_chart)

with col2:
    standort_pie_chart = px.pie(
        filtered_df,
        names="Standort",
        values="Wert",
        title="Distribution by Standort",
        hover_data={"Wert": ":.2f"},
    )
    standort_pie_chart.update_traces(hovertemplate="%{label}: %{value:.2f} (%{percent:.2%})")
    standort_pie_chart.update_layout(hoverlabel=dict(font_size=16))
    st.plotly_chart(standort_pie_chart)
