"""
Airport Operations Intelligence
===============================
Reads the GOLD layer built by dbt from OpenSky Network flight data.

Run inside the streamlit container, or locally with:
    streamlit run streamlit/dashboard.py
"""

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
import streamlit as st

st.set_page_config(
    page_title="Airport Operations Intelligence",
    page_icon="🛫",
    layout="wide",
)

TEMPLATE = "plotly_white"
ACCENT = "#2563EB"
MUTED = "#CBD5E1"
ALERT = "#DC2626"
CHART_HEIGHT = 340

st.markdown(
    """
    <style>
      .block-container {padding-top: 2.5rem; padding-bottom: 3rem;}
      [data-testid="stMetricValue"] {font-size: 1.6rem;}
    </style>
    """,
    unsafe_allow_html=True,
)


# ── Data access ───────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema="GOLD",
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        login_timeout=120,
        network_timeout=120,
    )


@st.cache_data(ttl=300)
def run_query(sql: str) -> pd.DataFrame:
    cursor = get_connection().cursor()
    try:
        cursor.execute(sql)
        columns = [d[0].lower() for d in cursor.description]
        frame = pd.DataFrame(cursor.fetchall(), columns=columns)
    finally:
        cursor.close()

    # Snowflake returns Decimal objects. Decimal and float cannot be mixed
    # in arithmetic, which breaks pandas and plotly, so convert numerics.
    for column in frame.columns:
        present = frame[column].notna()
        if not present.any():
            continue
        converted = pd.to_numeric(frame[column], errors="coerce")
        if converted[present].notna().all():
            frame[column] = converted
    return frame


# ── Queries ───────────────────────────────────────────────────────────
Q_AIRPORT_SUMMARY = """
SELECT airport_icao, city,
       SUM(total_arrivals)        AS arrivals,
       SUM(total_departures)      AS departures,
       SUM(total_movements)       AS total_movements,
       ROUND(AVG(total_movements), 0) AS avg_daily,
       MIN(flight_date)           AS data_from,
       MAX(flight_date)           AS data_to,
       COUNT(DISTINCT flight_date) AS days
FROM gld_daily_airport_traffic
GROUP BY 1, 2
ORDER BY total_movements DESC
"""

Q_DAILY_TREND = """
SELECT flight_date, airport_icao, city, total_movements
FROM gld_daily_airport_traffic
ORDER BY flight_date, airport_icao
"""

# Departures only: an arrival record carries the departure time of its
# origin airport, so including arrivals would place flights in the hour
# they left somewhere else entirely.
Q_HOURLY = """
SELECT airport_icao, city, departure_hour_local,
       SUM(flight_count) AS flights
FROM gld_hourly_flight_activity
WHERE flight_direction = 'departure'
GROUP BY 1, 2, 3
ORDER BY 1, 3
"""

# Unidentified operators are kept rather than filtered out, so market
# share is measured against all traffic instead of only the matched part.
Q_AIRLINES = """
SELECT airport_icao, city,
       COALESCE(airline_name, 'Unidentified operator') AS airline_name,
       COALESCE(alliance, 'Unknown')                   AS alliance,
       SUM(total_flights) AS flights
FROM gld_airline_traffic
GROUP BY 1, 2, 3, 4
ORDER BY airport_icao, flights DESC
"""

Q_ROUTES = """
SELECT departure_airport_icao, arrival_airport_icao,
       COALESCE(departure_city, departure_airport_icao) AS departure_city,
       COALESCE(arrival_city, arrival_airport_icao)     AS arrival_city,
       total_flights,
       ROUND(avg_duration_min, 0) AS avg_duration_min,
       unique_aircraft,
       days_with_service
FROM gld_route_analysis
ORDER BY total_flights DESC
"""

try:
    airport_df = run_query(Q_AIRPORT_SUMMARY)
    daily_df = run_query(Q_DAILY_TREND)
    hourly_df = run_query(Q_HOURLY)
    airline_df = run_query(Q_AIRLINES)
    routes_df = run_query(Q_ROUTES)
except Exception as error:
    st.error("Could not read from Snowflake. Check the warehouse is running and credentials are set.")
    st.exception(error)
    st.stop()

if airport_df.empty:
    st.warning("No data found in the GOLD schema. Run the pipeline first.")
    st.stop()


# ── Header and selector ───────────────────────────────────────────────
st.title("Airport Operations Intelligence")
st.caption(
    f"{airport_df['data_from'].iloc[0]} to {airport_df['data_to'].iloc[0]} · "
    f"{int(airport_df['days'].iloc[0])} days · Source: OpenSky Network"
)

city_map = dict(zip(airport_df["airport_icao"], airport_df["city"]))
selected = st.selectbox(
    "Airport",
    options=airport_df["airport_icao"].tolist(),
    format_func=lambda code: f"{code} ({city_map[code]})",
)
city = city_map[selected]
mine = airport_df[airport_df["airport_icao"] == selected].iloc[0]

st.divider()


# ── 1. Capacity overview ──────────────────────────────────────────────
st.header("1. Capacity overview")

arrivals = int(mine["arrivals"])
departures = int(mine["departures"])
movements = int(mine["total_movements"])
imbalance = departures - arrivals
imbalance_pct = 100 * imbalance / movements if movements else 0.0

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total movements", f"{movements:,}")
c2.metric("Average per day", f"{int(mine['avg_daily']):,}")
c3.metric("Arrivals", f"{arrivals:,}")
c4.metric("Departures", f"{departures:,}")
c5.metric("Direction skew", f"{imbalance_pct:+.1f}%", delta=f"{imbalance:+,} flights", delta_color="off")

if abs(imbalance_pct) >= 3:
    st.info(
        f"Departures exceed arrivals by {imbalance:+,} flights ({imbalance_pct:+.1f}%). "
        "Over a period this long an airport's arrivals and departures should balance, "
        "since every aircraft that lands leaves again. A gap this size is more likely "
        "to reflect uneven API coverage of the two directions than a real operational difference."
    )
else:
    st.caption(
        f"Arrivals and departures are within 3% of each other, which is what a "
        f"complete dataset for {city} should look like."
    )

st.divider()


# ── 2. Daily volume and peer comparison ───────────────────────────────
st.header("2. Daily volume and peer comparison")

left, right = st.columns([3, 2])
mine_daily = daily_df[daily_df["airport_icao"] == selected]
peer_daily = (
    daily_df[daily_df["airport_icao"] != selected]
    .groupby("flight_date", as_index=False)["total_movements"]
    .mean()
)

with left:
    trend = go.Figure()
    trend.add_trace(go.Scatter(
        x=mine_daily["flight_date"], y=mine_daily["total_movements"],
        name=city, mode="lines+markers",
        line=dict(color=ACCENT, width=3),
    ))
    trend.add_trace(go.Scatter(
        x=peer_daily["flight_date"], y=peer_daily["total_movements"],
        name="Average of the other five", mode="lines",
        line=dict(color=MUTED, width=2, dash="dash"),
    ))
    trend.update_layout(
        template=TEMPLATE, height=CHART_HEIGHT, margin=dict(t=10),
        xaxis_title="", yaxis_title="Daily movements",
        legend=dict(orientation="h", y=-0.2),
    )
    st.plotly_chart(trend, use_container_width=True)

with right:
    ranking = airport_df[["city", "avg_daily"]].sort_values("avg_daily")
    ranking_colours = [ACCENT if name == city else MUTED for name in ranking["city"]]
    rank_fig = go.Figure(go.Bar(
        x=ranking["avg_daily"], y=ranking["city"],
        orientation="h", marker_color=ranking_colours,
    ))
    rank_fig.update_layout(
        template=TEMPLATE, height=CHART_HEIGHT, margin=dict(t=10),
        xaxis_title="Average daily movements", yaxis_title="",
    )
    st.plotly_chart(rank_fig, use_container_width=True)

volumes = mine_daily["total_movements"].astype(float).tolist()
position = int((airport_df["avg_daily"] >= mine["avg_daily"]).sum())
notes = [f"{city} ranks {position} of {len(airport_df)} by average daily movements."]
if len(volumes) >= 6:
    first_three = sum(volumes[:3]) / 3
    last_three = sum(volumes[-3:]) / 3
    change = 100 * (last_three - first_three) / first_three if first_three else 0.0
    notes.append(
        f"Movements changed {change:+.1f}% between the first and last three days. "
        "A window this short cannot separate a trend from ordinary day to day variation."
    )
st.caption(" ".join(notes))

st.divider()


# ── 3. Departure peak hours ───────────────────────────────────────────
st.header("3. Departure peak hours")
st.caption(
    "Departure records only. An arrival record carries the departure time of its "
    "origin airport, so including arrivals would count a flight in the hour it left "
    "somewhere else. Local hours use a fixed UTC offset and do not account for daylight saving."
)

mine_hourly = hourly_df[hourly_df["airport_icao"] == selected]

if mine_hourly.empty:
    st.info("No hourly departure data for this airport.")
else:
    peak_row = mine_hourly.loc[mine_hourly["flights"].idxmax()]
    peak_flights = int(peak_row["flights"])
    peak_hour = int(peak_row["departure_hour_local"])
    total_hourly = int(mine_hourly["flights"].sum())

    busy = sorted(int(h) for h in mine_hourly.loc[mine_hourly["flights"] >= peak_flights * 0.85, "departure_hour_local"])
    quiet = sorted(int(h) for h in mine_hourly.loc[mine_hourly["flights"] < peak_flights * 0.3, "departure_hour_local"])
    active = mine_hourly.loc[mine_hourly["flights"] >= peak_flights * 0.3, "departure_hour_local"]

    chart_col, table_col = st.columns([3, 2])

    with chart_col:
        bar_colours = [ALERT if f >= peak_flights * 0.85 else ACCENT for f in mine_hourly["flights"]]
        hour_fig = go.Figure(go.Bar(
            x=mine_hourly["departure_hour_local"],
            y=mine_hourly["flights"],
            marker_color=bar_colours,
        ))
        hour_fig.update_layout(
            template=TEMPLATE, height=CHART_HEIGHT, margin=dict(t=10),
            xaxis_title="Hour, local time", yaxis_title="Departures",
            xaxis=dict(dtick=1),
        )
        st.plotly_chart(hour_fig, use_container_width=True)

    with table_col:
        def hours(values):
            return ", ".join(f"{h:02d}:00" for h in values) if values else "None"

        window = f"{int(active.min()):02d}:00 to {int(active.max()):02d}:00" if not active.empty else "Not available"
        st.markdown(
            f"""
            | Measure | Value |
            |---|---|
            | Busiest hour | **{peak_hour:02d}:00** |
            | Departures in that hour | {peak_flights:,} of {total_hourly:,} ({100 * peak_flights / total_hourly:.1f}%) |
            | Active window | {window} |
            | Within 15% of peak | {hours(busy)} |
            | Below 30% of peak | {hours(quiet)} |
            """
        )

    st.caption(
        f"{len(busy)} of the 24 hours sit within 15% of the busiest hour. "
        "Thresholds here are descriptive cut-offs for reading the chart, not capacity limits, "
        "which would need runway and stand data this dataset does not contain."
    )

st.divider()


# ── 4. Airline concentration ──────────────────────────────────────────
st.header("4. Airline concentration")

mine_airlines = airline_df[airline_df["airport_icao"] == selected].copy()

if mine_airlines.empty:
    st.info("No airline data for this airport.")
else:
    total_airline_flights = mine_airlines["flights"].sum()
    mine_airlines["share_pct"] = 100 * mine_airlines["flights"] / total_airline_flights
    identified = mine_airlines[mine_airlines["airline_name"] != "Unidentified operator"]
    unidentified_share = 100 - identified["share_pct"].sum()

    top_five = mine_airlines.head(5)
    pie_col, table_col = st.columns([2, 3])

    with pie_col:
        remainder = pd.DataFrame({
            "airline_name": ["Everyone else"],
            "flights": [total_airline_flights - top_five["flights"].sum()],
        })
        pie_data = pd.concat([top_five[["airline_name", "flights"]], remainder], ignore_index=True)
        pie = px.pie(pie_data, values="flights", names="airline_name", hole=0.45, template=TEMPLATE)
        pie.update_traces(textposition="inside", textinfo="percent")
        pie.update_layout(height=CHART_HEIGHT, margin=dict(t=10, b=10),
                          legend=dict(orientation="h", y=-0.1))
        st.plotly_chart(pie, use_container_width=True)

    with table_col:
        display = top_five[["airline_name", "alliance", "flights", "share_pct"]].copy()
        display["share_pct"] = display["share_pct"].round(1)
        display.columns = ["Airline", "Alliance", "Flights", "Share %"]
        st.dataframe(display, use_container_width=True, hide_index=True)

        hhi = int((identified["share_pct"] ** 2).sum())
        st.caption(
            f"Herfindahl-Hirschman Index across identified operators: **{hhi:,}**. "
            "Above 2500 is usually called highly concentrated and below 1500 competitive. "
            f"{unidentified_share:.1f}% of movements here could not be matched to an operator "
            "and are excluded from the index."
        )

    leader = mine_airlines.iloc[0]
    top_three_share = mine_airlines.head(3)["share_pct"].sum()
    st.caption(
        f"Largest operator: {leader['airline_name']} at {leader['share_pct']:.1f}% of movements. "
        f"Top three together: {top_three_share:.1f}%."
    )

st.divider()


# ── 5. Route connectivity ─────────────────────────────────────────────
st.header("5. Route connectivity")

touching = routes_df[
    (routes_df["departure_airport_icao"] == selected)
    | (routes_df["arrival_airport_icao"] == selected)
].copy()

if touching.empty:
    st.info("No routes recorded for this airport.")
else:
    top_routes = touching.head(10).copy()
    top_routes["route"] = top_routes["departure_city"] + " to " + top_routes["arrival_city"]
    top_routes["route_type"] = top_routes["avg_duration_min"].apply(
        lambda m: "Short haul, under 2h" if m < 120
        else ("Medium haul, 2 to 6h" if m < 360 else "Long haul, over 6h")
    )

    routes_col, mix_col = st.columns([3, 2])

    with routes_col:
        route_fig = px.bar(
            top_routes.sort_values("total_flights"),
            x="total_flights", y="route", orientation="h",
            color="avg_duration_min", color_continuous_scale="Blues",
            labels={"total_flights": "Flights", "route": "", "avg_duration_min": "Average minutes"},
            template=TEMPLATE,
        )
        route_fig.update_layout(height=420, margin=dict(t=10), yaxis=dict(dtick=1))
        st.plotly_chart(route_fig, use_container_width=True)

    with mix_col:
        mix = top_routes.groupby("route_type", as_index=False)["total_flights"].sum()
        mix_fig = px.pie(
            mix, values="total_flights", names="route_type", hole=0.45,
            color="route_type",
            color_discrete_map={
                "Short haul, under 2h": "#10B981",
                "Medium haul, 2 to 6h": ACCENT,
                "Long haul, over 6h": ALERT,
            },
            template=TEMPLATE,
        )
        mix_fig.update_traces(textposition="inside", textinfo="percent")
        mix_fig.update_layout(height=280, margin=dict(t=10, b=10),
                              legend=dict(orientation="h", y=-0.15))
        st.plotly_chart(mix_fig, use_container_width=True)

        destinations = set(
            touching.loc[touching["departure_airport_icao"] == selected, "arrival_airport_icao"]
        ) | set(
            touching.loc[touching["arrival_airport_icao"] == selected, "departure_airport_icao"]
        )
        m1, m2 = st.columns(2)
        m1.metric("Routes", f"{len(touching):,}")
        m2.metric("Destinations", f"{len(destinations):,}")

    st.caption(
        "Flights between two monitored airports appear in this dataset twice, once as a "
        "departure and once as an arrival, so their counts are inflated relative to routes "
        "with only one monitored endpoint. See the data notes below."
    )

st.divider()


# ── Data notes ────────────────────────────────────────────────────────
with st.expander("Data notes and known limitations"):
    st.markdown(
        """
- **Source.** OpenSky Network, a volunteer ADS-B receiver network. Coverage is strong over
  Europe and North America and thinner elsewhere, so counts are a lower bound on real traffic.
- **Availability lag.** A day of flight records is only complete once roughly 24 hours have
  passed. Measured on this data: a window ending 3 hours ago returns about 8% of a day,
  12 hours ago about 61%, and 24 hours ago 100%. Departures are published before arrivals,
  so a query made too early is skewed toward departures rather than uniformly incomplete.
- **Unidentified operators.** Airlines are matched by the three letter ICAO prefix of the
  callsign. About 11% of movements do not match a known operator, mostly cargo, charter,
  business and military traffic. Some carriers file IATA style callsigns, which the prefix
  rule cannot parse.
- **Route double counting.** A flight between two monitored airports is recorded once as a
  departure and once as an arrival, so those routes are counted twice in section 5.
- **Local time.** Hours use a fixed UTC offset per airport and do not adjust for daylight
  saving, so summer local hours may be one hour out.
- **No commercial data.** OpenSky tracks aircraft, not tickets. There are no passenger
  counts, fares, delays or reasons for delay.
        """
    )

st.caption("Data: OpenSky Network · Pipeline: Airflow, Snowflake, dbt, Streamlit")