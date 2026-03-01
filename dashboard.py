# -*- coding: utf-8 -*-
import streamlit as st
import sqlite3
import pandas as pd
import time
import os

REFRESH_SECONDS = 15

st.set_page_config(page_title="Arbitrage Scanner", page_icon="🚀", layout="wide")
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Database', 'arbitrage_dashboard.db')


def load_data():
    if not os.path.exists(DB_PATH): return pd.DataFrame()
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        df = pd.read_sql_query("SELECT * FROM live_opportunities", conn)
        conn.close()
        return df
    except:
        return pd.DataFrame()


def get_trade_url(exchange, token):
    ex, t = exchange.lower().strip(), token.upper().strip()
    if 'lighter' in ex:
        return f"https://app.Lighter.xyz/trade/{t}/?referral=118787PQ"
    elif 'paradex' in ex:
        return f"https://app.Paradex.trade/trade/{t}-USD-PERP"
    elif 'variational' in ex or 'omni' in ex:
        return f"https://omni.Variational.io/perpetual/{t}"
    elif 'backpack' in ex:
        return f"https://Backpack.exchange/trade/{t}_USD_PERP"
    elif 'extended' in ex:
        return f"https://app.Extended.exchange/perp/{t}-USD"
    return f"https://www.google.com/search?q={exchange.capitalize()}+{t}+perp"


st.title("🚀 Live Arbitrage Dashboard")
df = load_data()

with st.container(border=True):
    col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 2, 1])
    with col1: min_spread = st.number_input("📉 Мін. спред (%)", value=-100.0, step=0.1)
    with col2: hold_hours = st.number_input("⏱️ Години холду", value=8.0, step=1.0)
    with col3: search_token = st.multiselect("Coin", sorted(df['token'].unique()) if not df.empty else [],
                                             placeholder="Всі")
    with col4: selected_exchanges = st.multiselect("Exchanges", sorted(
        set(df['buy_exchange'].unique()) | set(df['sell_exchange'].unique())) if not df.empty else [],
                                                   placeholder="Всі")
    with col5:
        st.markdown("<br>", unsafe_allow_html=True)
        auto_refresh = st.toggle("🔄 Авто-оновлення", value=True)
        timer_placeholder = st.empty()
        if st.button("Оновити"): st.rerun()

if not df.empty:
    df_filtered = df[df['spread_pct'] >= min_spread].copy()

    # Фільтр по токенам
    if search_token:
        df_filtered = df_filtered[df_filtered['token'].isin(search_token)]

    # 🔥 ВИПРАВЛЕНИЙ: Розумний фільтр бірж
    if selected_exchanges:
        if len(selected_exchanges) == 1:
            # Режим 1 біржі: Показуємо всі зв'язки з нею (АБО)
            mask = df_filtered['buy_exchange'].isin(selected_exchanges) | df_filtered['sell_exchange'].isin(
                selected_exchanges)
        else:
            # Режим 2+ бірж: Показуємо маршрути ТІЛЬКИ між ними (І)
            mask = df_filtered['buy_exchange'].isin(selected_exchanges) & df_filtered['sell_exchange'].isin(
                selected_exchanges)

        df_filtered = df_filtered[mask]

    buy_hourly = df_filtered['buy_funding_rate'] / df_filtered['buy_funding_freq']
    sell_hourly = df_filtered['sell_funding_rate'] / df_filtered['sell_funding_freq']
    net_hourly = sell_hourly - buy_hourly
    df_filtered['funding_apr'] = net_hourly * 24 * 365
    df_filtered['f_spread_expected'] = net_hourly * hold_hours
    df_filtered['total_expected_profit'] = df_filtered['spread_pct'] + df_filtered['f_spread_expected']

    df_filtered = df_filtered.sort_values(by='total_expected_profit', ascending=False)
    df_filtered['buy_link'] = df_filtered.apply(lambda r: get_trade_url(r['buy_exchange'], r['token']), axis=1)
    df_filtered['sell_link'] = df_filtered.apply(lambda r: get_trade_url(r['sell_exchange'], r['token']), axis=1)

    m1, m2, m3 = st.columns(3)
    m1.metric("Маршрутів", len(df_filtered))
    if not df_filtered.empty:
        m2.metric("Топ спред", f"{df_filtered.iloc[0]['spread_pct']:.2f}%")
        m3.metric("Топ пара", f"{df_filtered.iloc[0]['token']} ({df_filtered.iloc[0]['route']})")

    display_cols = [
        'token', 'buy_link', 'sell_link', 'total_expected_profit', 'spread_pct',
        'funding_apr', 'f_spread_expected',
        'buy_funding_rate', 'buy_funding_freq', 'buy_funding_24h_pct',
        'sell_funding_rate', 'sell_funding_freq', 'sell_funding_24h_pct',
        'spread_min_24h', 'spread_max_24h', 'buy_price', 'sell_price'
    ]

    clean_regex = r"https?://(?:www\.|app\.|omni\.)?(\w+)"
    config = {
        "token": st.column_config.TextColumn("Token", width="small"),
        "buy_link": st.column_config.LinkColumn("Buy Route", display_text=clean_regex, width="medium"),
        "sell_link": st.column_config.LinkColumn("Sell Route", display_text=clean_regex, width="medium"),
        "total_expected_profit": st.column_config.NumberColumn("Total Expected", format="%.2f %%"),
        "spread_pct": st.column_config.NumberColumn("Spread", format="%.2f %%"),
        "funding_apr": st.column_config.NumberColumn("Fund APR", format="%.2f %%"),
        "f_spread_expected": st.column_config.NumberColumn(f"F_spread {int(hold_hours)}h", format="%.4f %%"),
        "buy_funding_rate": st.column_config.NumberColumn("Buy Fund", format="%.4f %%"),
        "buy_funding_freq": st.column_config.NumberColumn("Buy Freq (h)"),
        "buy_funding_24h_pct": st.column_config.NumberColumn("F buy 24h", format="%.4f %%"),
        "sell_funding_rate": st.column_config.NumberColumn("Sell Fund", format="%.4f %%"),
        "sell_funding_freq": st.column_config.NumberColumn("Sell Freq (h)"),
        "sell_funding_24h_pct": st.column_config.NumberColumn("F sell 24h", format="%.4f %%"),
        "spread_min_24h": st.column_config.NumberColumn("Min 24h", format="%.2f %%"),
        "spread_max_24h": st.column_config.NumberColumn("Max 24h", format="%.2f %%"),
        "buy_price": st.column_config.NumberColumn("Buy Price", format="%.4f"),
        "sell_price": st.column_config.NumberColumn("Sell Price", format="%.4f"),
    }


    def hl(v):
        return 'background-color: #d4edda; color: black;' if v > 0.5 else 'background-color: #fff3cd; color: black;' if v > 0 else 'background-color: #f8d7da; color: black;'


    st.dataframe(df_filtered[display_cols].style.map(hl, subset=['total_expected_profit']), width="stretch", height=800,
                 column_config=config, hide_index=True)

else:
    st.info("⏳ Очікування даних...")

if auto_refresh:
    for i in range(REFRESH_SECONDS, 0, -1):
        timer_placeholder.markdown(f"⏳ Оновлення: **{i}** с")
        time.sleep(1)
    st.rerun()
else:
    timer_placeholder.markdown("⏸️ **Пауза**")